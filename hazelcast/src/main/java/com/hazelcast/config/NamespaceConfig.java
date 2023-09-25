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

package com.hazelcast.config;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceConfig implements NamedConfig {
    @Nullable
    private String name;

    private final Map<String, ResourceConfig> resourceConfigs = new ConcurrentHashMap<>();

    public NamespaceConfig() {
    }

    public NamespaceConfig(String name) {
        this.name = name;
    }

    @Override
    public NamespaceConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Deprecated
    public NamespaceConfig addClass(@Nonnull String className, @Nonnull File classFile) {
        // TODO:
        // The current addClass(String, File) method is flawed. This style of configuration allows to add a class as resource
        // from an external file (external in the sense that the class is not loaded as part of your app’s classpath). But the
        // flaw is this: user gives a class name and a .class file but we cannot figure out if there are anonymous or named
        // inner classes until we actually load that class (so no respective ResourceConfigs can be created for those inner
        // classes). OTOH I see JobConfig only provides addClass(Class...) API - this allows to reflectively figure out inner
        // classes at configuration time and add the respective ResourceConfigs in our internal data structure. I think the
        // JobConfig approach makes more sense and we should implement that one.
        try {
            add(classFile.toURI().toURL(), className, ResourceType.JAR);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public NamespaceConfig addClass(@Nonnull Class<?>... classes) {
        checkNotNull(classes, "Classes cannot be null");
        ResourceConfig.fromClass(classes).forEach(cfg -> resourceConfigs.put(cfg.getId(), cfg));
        return this;
    }

    public NamespaceConfig addJar(@Nonnull URL url) {
       return add(url, null, ResourceType.JAR);
    }

    public NamespaceConfig addJarsInZip(@Nonnull URL url) {
        return add(url, null, ResourceType.JARS_IN_ZIP);
    }

    private NamespaceConfig add(@Nonnull URL url, @Nullable String id, @Nonnull ResourceType resourceType) {
        final ResourceConfig cfg = new ResourceConfig(url, id, resourceType);

        if (resourceConfigs.putIfAbsent(id, cfg) != null) {
            throw new IllegalArgumentException("Resource with id:" + id + " already exists");
        } else {
            return this;
        }
    }

    public NamespaceConfig removeResourceConfig(String id) {
        resourceConfigs.remove(id);
        return this;
    }

    // TODO Should this return a Set?
    // There's no guarantee that the resourceConfig#values are actually unique
    // I think it should be a Collection
    Set<ResourceConfig> getResourceConfigs() {
        return Collections.unmodifiableSet(new HashSet<>(resourceConfigs.values()));
    }
}
