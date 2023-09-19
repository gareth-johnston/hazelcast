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

/**
 * A ClassLoader that's aware of the configured namespaces and their resources.
 * The classloading scheme does not follow the recommended {@link ClassLoader} parent delegation model: instead, this
 * {@code ClassLoader} first looks up classes and resources on its own, then delegates if not found.
 *
 * @see com.hazelcast.config.NamespaceConfig
 */
public class NamespaceAwareClassLoader extends ClassLoader {

    private final NamespaceServiceImpl namespaceService;

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public NamespaceAwareClassLoader(ClassLoader parent, NamespaceServiceImpl namespaceService) {
        super(parent);
        this.namespaceService = namespaceService;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            ClassLoader candidate = pickClassLoader();
            Class<?> klass = candidate.loadClass(name);
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return super.findClass(name);
    }

    ClassLoader pickClassLoader() {
        String namespace = NamespaceThreadLocalContext.getNamespaceThreadLocalContext();
        if (namespace == null) {
            return getParent();
        } else {
            ClassLoader candidate = namespaceService.namespaceToClassLoader.get(namespace);
            return candidate == null ? getParent() : candidate;
        }
    }
}
