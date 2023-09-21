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

    public NamespaceConfig addJar(@Nonnull URL url) {
        // TODO
        throw new UnsupportedOperationException();
    }

    public NamespaceConfig addJarsInZip(@Nonnull URL url) {
        // TODO
        throw new UnsupportedOperationException();
    }

    public NamespaceConfig removeResourceConfig(String id) {
        resourceConfigs.remove(id);
        return this;
    }

    Set<ResourceConfig> getResourceConfigs() {
        return Collections.unmodifiableSet(new HashSet(resourceConfigs.values()));
    }
}
