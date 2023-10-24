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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceConfig implements NamedConfig, IdentifiedDataSerializable {
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

    public NamespaceConfig addClass(@Nonnull Class<?>... classes) {
        Objects.requireNonNull(classes, "Classes cannot be null");
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

        if (resourceConfigs.putIfAbsent(cfg.getId(), cfg) != null) {
            throw new IllegalArgumentException("Resource with id: " + cfg.getId() + " already exists");
        } else {
            return this;
        }
    }

    public NamespaceConfig removeResourceConfig(String id) {
        resourceConfigs.remove(id);
        return this;
    }

    public Collection<ResourceConfig> getResourceConfigs() {
        return Set.copyOf(resourceConfigs.values());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        SerializationUtil.writeMapStringKey(resourceConfigs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            resourceConfigs.put(in.readString(), in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.NAMESPACE_CONFIG;
    }
}
