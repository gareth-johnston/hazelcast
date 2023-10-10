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

import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;

public class NamespaceServiceImpl implements NamespaceService {
    private static final Pattern CLASS_PATTERN = Pattern.compile("(.*)\\.class$");

    final ConcurrentMap<String, MapResourceClassLoader> namespaceToClassLoader
            = new ConcurrentHashMap<>();

    private final ClassLoader configClassLoader;

    public NamespaceServiceImpl(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
    }

    @Override
    public void addNamespace(@Nonnull String nsName,
                             @Nonnull Set<ResourceDefinition> resources) {
        Objects.requireNonNull(nsName, "namespace name cannot be null");
        Objects.requireNonNull(resources, "resources cannot be null");

        Map<String, byte[]> resourceMap = new ConcurrentHashMap<>();
        for (ResourceDefinition r : resources) {
            handleResource(r, resourceMap);
        }

        MapResourceClassLoader updated = new MapResourceClassLoader(configClassLoader,
                () -> resourceMap, true);

        MapResourceClassLoader removed = namespaceToClassLoader.put(nsName, updated);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
        }
        initializeClassLoader(nsName, updated);
    }

    @Override
    public boolean removeNamespace(@Nonnull String nsName) {
        MapResourceClassLoader removed = namespaceToClassLoader.remove(nsName);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
            return true;
        } else {
            return false;
        }
    }

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

    public static void handleJar(String id, byte[] jarBytes, Map<String, byte[]> resourceMap) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(jarBytes);
             JarInputStream inputStream = new JarInputStream(bais)) {
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                // todo completely inefficient, doing pattern matching twice currently
                String entryName = extractClassName(entry);
                boolean isClass = false;
                Matcher matcher = CLASS_PATTERN.matcher(entry.getName().replace('/', '.'));
                if (matcher.matches()) {
                    isClass = true;
                }
                baos.reset();
                try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
                    IOUtil.drainTo(inputStream, compressor);
                }
                inputStream.closeEntry();
                byte[] payload = baos.toByteArray();
                resourceMap.put(isClass
                        ? JobRepository.classKeyName(toClassResourceId(entryName))
                        : JobRepository.fileKeyName(entryName), payload);
            } while (true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read from JAR bytes for resource with id "
                    + id, e);
        }
    }

    /**
     *
     * @param resourceId the resource ID for the class, ie fully qualified class name converted to path, suffixed with ".class"
     * @param classBytes the class binary content
     * @param resourceMap resource map to add resource to
     * @see com.hazelcast.jet.impl.util.ReflectionUtils#toClassResourceId
     */
    void handleClass(String resourceId, byte[] classBytes, Map<String, byte[]> resourceMap) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos);
                ByteArrayInputStream inputStream = new ByteArrayInputStream(classBytes)) {
            IOUtil.drainTo(inputStream, compressor);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read class bytes for resource with id "
                    + resourceId, e);
        }
        byte[] classDefinition = baos.toByteArray();
        resourceMap.put(JobRepository.classKeyName(resourceId), classDefinition);
    }

    static String extractClassName(JarEntry entry) {
        String entryName = entry.getName();
        Matcher matcher = CLASS_PATTERN.matcher(entryName.replace('/', '.'));
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return entry.getName();
    }

    private void initializeClassLoader(String nsName, MapResourceClassLoader classLoader) {
        initializeJdbcDrivers(nsName, classLoader);
    }

    private void cleanUpClassLoader(String nsName, MapResourceClassLoader removedClassLoader) {
        cleanupJdbcDrivers(nsName, removedClassLoader);
    }

    private static void initializeJdbcDrivers(String nsName, MapResourceClassLoader classLoader) {
        ServiceLoader<? extends Driver> driverLoader = ServiceLoader.load(Driver.class, classLoader);
        for (Driver d : driverLoader) {
            // todo proper logging
            if (d.getClass().getClassLoader() != classLoader) {
                continue;
            }
            System.out.println("Registering driver " + d.getClass() + " from classloader for namespace " + nsName);
            try {
                DriverManager.registerDriver(d);
            } catch (SQLException e) {
                // todo proper logger needed here
                e.printStackTrace();
            }
        }
    }

    private static void cleanupJdbcDrivers(String nsName, MapResourceClassLoader removedClassLoader) {
        // cleanup any JDBC drivers that were registered from that classloader
        Enumeration<Driver> registeredDrivers = DriverManager.getDrivers();
        while (registeredDrivers.hasMoreElements()) {
            Driver d = registeredDrivers.nextElement();
            if (d.getClass().getClassLoader() == removedClassLoader) {
                try {
                    // todo proper logger needed here
                    System.out.println("Deregistering " + d.getClass() + " from removed classloader for namespace " + nsName);
                    DriverManager.deregisterDriver(d);
                } catch (SQLException e) {
                    // todo proper logger needed here
                    e.printStackTrace();
                }
            }
        }
    }
}
