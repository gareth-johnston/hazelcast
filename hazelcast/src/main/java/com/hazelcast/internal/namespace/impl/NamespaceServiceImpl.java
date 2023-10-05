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
import java.util.Map;
import java.util.Objects;
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
//        if (removed != null) {
//            // todo: clean up of removed classloader???
//        }
    }

    @Override
    public boolean removeNamespace(@Nonnull String nsName) {
        MapResourceClassLoader removed = namespaceToClassLoader.remove(nsName);
//        if (removed != null) {
//            // todo: clean up of removed classloader
//            return true;
//        } else {
//            return false;
//        }
        return removed != null;
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

    void handleJar(String id, byte[] jarBytes, Map<String, byte[]> resourceMap) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(jarBytes);
             JarInputStream inputStream = new JarInputStream(bais)) {
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                String className = extractClassName(entry);
                if (className == null) {
                    continue;
                }
                baos.reset();
                try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
                    IOUtil.drainTo(inputStream, compressor);
                }
                inputStream.closeEntry();
                byte[] classDefinition = baos.toByteArray();
                resourceMap.put(JobRepository.classKeyName(toClassResourceId(className)), classDefinition);
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

    private String extractClassName(JarEntry entry) {
        String entryName = entry.getName();
        Matcher matcher = CLASS_PATTERN.matcher(entryName.replace('/', '.'));
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }
}
