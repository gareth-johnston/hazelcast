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

import com.hazelcast.jet.config.ResourceConfig;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceConfig implements NamedConfig {

    private String name;

    private final Set<ResourceConfig> resourceConfigs = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public NamedConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    public NamespaceConfig addResourceConfig(ResourceConfig resourceConfig) {
        resourceConfigs.add(resourceConfig);
        return this;
    }

    // todo: maybe remove(String id)?
    public NamespaceConfig removeResourceConfig(ResourceConfig resourceConfig) {
        resourceConfigs.remove(resourceConfig);
        return this;
    }

    Set<ResourceConfig> getResourceConfigs() {
        return Collections.unmodifiableSet(resourceConfigs);
    }
}
