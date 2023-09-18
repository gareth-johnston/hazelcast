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
import com.hazelcast.jet.config.ResourceType;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceConfig implements NamedConfig {

    private String name;

    private final Map<String, ResourceConfig> resourceConfigs = new ConcurrentHashMap<>();

    @Override
    public NamedConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    public NamespaceConfig addClass(String className, File classFile) {
        try {
            // todo: is ResourceConfig an appropriate internal representation of resources until we need
            //  to actually read them into byte[]'s and feed them to internal namespace classloaders impl?
            //  If yes, definitely reusing `ResourceConfig` but making its constructor public is not a good option
            resourceConfigs.put(className,
                    new ResourceConfig(classFile.toURI().toURL(), className, ResourceType.CLASS));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public NamespaceConfig removeResourceConfig(String id) {
        resourceConfigs.remove(id);
        return this;
    }

    Set<ResourceConfig> getResourceConfigs() {
        return Collections.unmodifiableSet(new HashSet(resourceConfigs.values()));
    }
}
