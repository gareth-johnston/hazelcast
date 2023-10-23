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
import com.hazelcast.internal.util.ExceptionUtil;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.net.URL;
import java.util.Enumeration;

/**
 * A ClassLoader that's aware of the configured namespaces and their resources.
 * The classloading scheme does not follow the recommended {@link ClassLoader} parent delegation model: instead, this
 * {@code ClassLoader} first looks up classes and resources on its own, then delegates if not found.
 *
 * @see com.hazelcast.config.NamespaceConfig
 */
public class NamespaceAwareClassLoader extends ClassLoader {
    private static final MethodHandle FIND_RESOURCE_METHOD_HANDLE;
    private static final MethodHandle FIND_RESOURCES_METHOD_HANDLE;

    private final NamespaceService namespaceService;

    static {
        try {
            ClassLoader.registerAsParallelCapable();

            Lookup privateLookup = MethodHandles.privateLookupIn(ClassLoader.class, MethodHandles.lookup());

            FIND_RESOURCE_METHOD_HANDLE = privateLookup
                    .unreflect(ClassLoader.class.getDeclaredMethod("findResource", String.class));
            FIND_RESOURCES_METHOD_HANDLE = privateLookup
                    .unreflect(ClassLoader.class.getDeclaredMethod("findResources", String.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public NamespaceAwareClassLoader(ClassLoader parent, NamespaceService namespaceService) {
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
    protected URL findResource(String name) {
        try {
            return (URL) FIND_RESOURCE_METHOD_HANDLE.invoke(pickClassLoader(), name);
        } catch (Throwable t) {
            throw ExceptionUtil.sneakyThrow(t);
        }
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        try {
            return (Enumeration<URL>) FIND_RESOURCES_METHOD_HANDLE.invoke(pickClassLoader(), name);
        } catch (Throwable t) {
            throw ExceptionUtil.sneakyThrow(t);
        }
    }

    ClassLoader pickClassLoader() {
        String namespace = NamespaceThreadLocalContext.getNamespaceThreadLocalContext();
        if (namespace == null) {
            return getParent();
        } else {
            ClassLoader candidate = namespaceService.getClassLoaderForNamespace(namespace);
            return candidate == null ? getParent() : candidate;
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return pickClassLoader().getResources(name);
    }

    // TODO: Should this be registered as a proper service and accessible via #getService() call?
    public NamespaceService getNamespaceService() {
        return namespaceService;
    }
}
