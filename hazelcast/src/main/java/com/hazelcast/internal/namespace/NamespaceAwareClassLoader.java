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

package com.hazelcast.internal.namespace;

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A ClassLoader that's aware of the configured namespaces and their resources.
 * The classloading scheme does not follow the recommended {@link ClassLoader} parent delegation model: instead, this
 * {@code ClassLoader} first looks up classes and resources on its own, then delegates if not found.
 *
 * @see com.hazelcast.config.NamespaceConfig
 */
public class NamespaceAwareClassLoader extends ClassLoader {

    // namespace name -> classloader
    private final ConcurrentMap<String, JetClassLoader> namespaceClassLoaders = new ConcurrentHashMap<>();

    public NamespaceAwareClassLoader(ClassLoader parent) {
        super(parent);
        ClassLoader.registerAsParallelCapable();
    }

    void addNamespace(NamespaceConfig namespaceConfig) {
        // JetClassLoader jetClassLoader = new JetClassLoader();
    }

}
