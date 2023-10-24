/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;

public class NamespaceServiceImpl implements NamespaceService {
    private static final ILogger LOGGER = Logger.getLogger(NamespaceServiceImpl.class);

    final ConcurrentMap<String, MapResourceClassLoader> namespaceToClassLoader = new ConcurrentHashMap<>();

    private final ClassLoader configClassLoader;
    private boolean hasDefaultNamespace;

    public NamespaceServiceImpl(ClassLoader configClassLoader, Map<String, NamespaceConfig> nsConfigs) {
        this.configClassLoader = configClassLoader;
        nsConfigs.forEach((nsName, nsConfig) -> addNamespace(nsName, resourceDefinitions(nsConfig)));
    }

    @Override
    public void addNamespace(@Nonnull String nsName, @Nonnull Collection<ResourceDefinition> resources) {
        Objects.requireNonNull(nsName, "namespace name cannot be null");
        Objects.requireNonNull(resources, "resources cannot be null");

        Map<String, byte[]> resourceMap = new ConcurrentHashMap<>();
        for (ResourceDefinition r : resources) {
            handleResource(r, resourceMap);
        }

        MapResourceClassLoader updated = new MapResourceClassLoader(configClassLoader, () -> resourceMap, true);

        MapResourceClassLoader removed = namespaceToClassLoader.put(nsName, updated);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
        }
        initializeClassLoader(nsName, updated);
        if (nsName.equals(DEFAULT_NAMESPACE_ID)) {
            hasDefaultNamespace = true;
        }
    }

    @Override
    public boolean removeNamespace(@Nonnull String nsName) {
        MapResourceClassLoader removed = namespaceToClassLoader.remove(nsName);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
            if (nsName.equals(DEFAULT_NAMESPACE_ID)) {
                hasDefaultNamespace = false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean hasNamespace(String namespaceName) {
        return namespaceToClassLoader.containsKey(namespaceName);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isDefaultNamespaceDefined() {
        return hasDefaultNamespace;
    }

    // Namespace setup/cleanup handling functions

    private void setupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onStartNsAware(namespace);
    }

    private void cleanupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onCompleteNsAware(namespace);
    }

    @Override
    public void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        namespace = transformNamespace(namespace);
        setupNs(namespace);
        try {
            runnable.run();
        } finally {
            cleanupNs(namespace);
        }
    }

    @Override
    public <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        namespace = transformNamespace(namespace);
        setupNs(namespace);
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        } finally {
            cleanupNs(namespace);
        }
    }

    // Internal method to transform a `null` namespace into the default namespace if available
    private String transformNamespace(String namespace) {
        if (namespace != null) {
            return namespace;
            // Check if we have a `default` environment available
        } else if (isDefaultNamespaceDefined()) {
            return DEFAULT_NAMESPACE_ID;
        } else {
            // Namespace is null, no default Namespace is defined, fail-fast
            return null;
        }
    }

    // Resource/classloader handling functions

    void handleResource(ResourceDefinition resource, Map<String, byte[]> resourceMap) {
        switch (resource.type()) {
            case JAR:
                handleJar(resource.id(), resource.payload(), resourceMap);
                break;
            case CLASS:
                handleClass(resource.id(), resource.payload(), resourceMap);
                break;
            default:
                throw new IllegalArgumentException("Cannot handle resource type " + resource.type());
        }
    }

    /**
     * Add classes and files in the given {@code jarBytes} to the provided {@code resourceMap}, after appropriate
     * encoding:
     * <ul>
     *     <li>Payload is deflated</li>
     *     <li>For each JAR entry in {@code jarBytes} that is a class, its class name is converted to a resource ID with the
     *     {@code "c."} prefix followed by the class name converted to a path.</li>
     *     <li>For other JAR entries in {@code jarBytes}, its path is converted to a resource ID with the
     *     {@code "f."} prefix followed by the path.</li>
     * </ul>
     * @param id
     * @param jarBytes
     * @param resourceMap
     * @see     com.hazelcast.jet.impl.util.ReflectionUtils#toClassResourceId(String)
     * @see     JobRepository#classKeyName(String)
     */
    public static void handleJar(String id, byte[] jarBytes, Map<String, byte[]> resourceMap) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(jarBytes);
                JarInputStream inputStream = new JarInputStream(bais)) {
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }
                String className = ClassLoaderUtil.extractClassName(entry.getName());
                byte[] payload = IOUtil.compress(inputStream.readAllBytes());
                inputStream.closeEntry();
                resourceMap.put(className == null ? JobRepository.fileKeyName(entry.getName())
                        : classKeyName(toClassResourceId(className)), payload);
            } while (true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read from JAR bytes for resource with id " + id, e);
        }
    }

    /**
     * Add the class with given {@code resourceId} to the {@code resourceMap}, after performing deflate compression on its
     * payload.
     * @param resourceId the resource ID for the class, ie fully qualified class name converted to path, suffixed with ".class"
     * @param classBytes the class binary content
     * @param resourceMap resource map to add resource to
     * @see com.hazelcast.jet.impl.util.ReflectionUtils#toClassResourceId
     */
    public static void handleClass(String resourceId, byte[] classBytes, Map<String, byte[]> resourceMap) {
        resourceMap.put(classKeyName(resourceId), IOUtil.compress(classBytes));
    }

    private void initializeClassLoader(String nsName, MapResourceClassLoader classLoader) {
        initializeJdbcDrivers(nsName, classLoader);
    }

    private void cleanUpClassLoader(String nsName, MapResourceClassLoader removedClassLoader) {
        cleanupJdbcDrivers(nsName, removedClassLoader);
    }

    private static void initializeJdbcDrivers(String nsName, MapResourceClassLoader classLoader) {
        ServiceLoader<? extends Driver> driverLoader = ServiceLoader.load(Driver.class, classLoader);

        LOGGER.finest("Initializing driverLoader=" + driverLoader + ", in namespace " + nsName);

        for (Driver d : driverLoader) {
            if (d.getClass().getClassLoader() == classLoader) {
                LOGGER.finest("Registering driver " + d.getClass() + " from classloader for namespace " + nsName);

                try {
                    DriverManager.registerDriver(d);
                } catch (SQLException e) {
                    LOGGER.warning("Failed to register driver " + d + " in namespace " + nsName, e);
                }
            } else {
                LOGGER.finest("Skipping " + d.getClass() + " because it's classloader (" + d.getClass().getClassLoader()
                        + ") differs from classloader (" + classLoader + ")");
            }
        }
    }

    /** cleanup any JDBC drivers that were registered from that classloader */
    private static void cleanupJdbcDrivers(String nsName, MapResourceClassLoader removedClassLoader) {
        Enumeration<Driver> registeredDrivers = DriverManager.getDrivers();

        LOGGER.finest("Cleaning up registeredDrivers=" + registeredDrivers + ", in namespace " + nsName);

        while (registeredDrivers.hasMoreElements()) {
            Driver d = registeredDrivers.nextElement();

            if (d.getClass().getClassLoader() == removedClassLoader) {
                try {
                    LOGGER.finest("Deregistering " + d.getClass() + " from removed classloader for namespace " + nsName);
                    DriverManager.deregisterDriver(d);
                } catch (SQLException e) {
                    LOGGER.warning("Failed to deregister driver " + d + " in namespace " + nsName, e);
                }
            } else {
                LOGGER.finest("Skipping " + d.getClass() + " because it's classloader (" + d.getClass().getClassLoader()
                        + ") differs from removedClassLoader (" + removedClassLoader + ")");
            }
        }
    }

    private static Collection<ResourceDefinition> resourceDefinitions(NamespaceConfig nsConfig) {
        return ConfigAccessor.getResourceConfigs(nsConfig).stream().map(ResourceDefinitionImpl::new)
                .collect(Collectors.toSet());
    }

    @Override
    public MapResourceClassLoader getClassLoaderForNamespace(String namespace) {
        return namespaceToClassLoader.get(namespace);
    }
}
