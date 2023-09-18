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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * Abstract class loader that can be customized with:
 * <ul>
 *     <li>A resources supplier. For now it's a {@code Supplier<? extends Map<String, byte[]>>}. In the concrete
 *     {@code JetClassLoader} case, it is backed by an {@code IMap<String, byte[]>}.</li>
 *     <li>Choice of whether it looks up classes and resources in child-first order (so this ClassLoader's resources are
 *     first search, then, if not found, the parent ClassLoader is queries). If not child-first, then the common parent-first
 *     hierarchical ClassLoader model is followed.</li>
 *     <li>TODO: NOT HAPPENING CURRENTLY:
 *      A mechanism to create resource URLs that can be resolved to streams by this {@code ClassLoader}'s resources.</li>
 * </ul>
 * todo: specify resource MAP key & value format. Currently following JetClassLoader conventions:
 *  <ul>
 *      <li>key if "c." + classname as file path + ".class". See also JobRepository class for other key prefixes</li>
 *      <li>value is deflated class definition</li>
 *  </ul>
 *  todo: consider if we need to override java9+ methods for running on the modulepath use case
 */
public class MapResourceClassLoader extends JetDelegatingClassLoader {

    // todo: consider alternative to IMap
    //  take into account potential deadlocks like https://hazelcast.atlassian.net/browse/HZ-3121
    protected final Supplier<? extends Map<String, byte[]>> resourcesSupplier;
    /**
     * When {@code true}, if the requested class/resource is not found in this ClassLoader's resources, then parent
     * is queried. Otherwise, only resources in this ClassLoader are searched.
     */
    protected final boolean childFirst;
    protected volatile boolean isShutdown;

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public MapResourceClassLoader(ClassLoader parent,
                                     @Nonnull Supplier<? extends Map<String, byte[]>> resourcesSupplier,
                                     boolean childFirst) {
        super(parent);
        this.resourcesSupplier = Util.memoizeConcurrent(resourcesSupplier);
        this.childFirst = childFirst;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!childFirst) {
            return super.loadClass(name, resolve);
        }
        synchronized (getClassLoadingLock(name)) {
            Class<?> klass = findLoadedClass(name);
            // first lookup class in own resources
            try {
                if (klass == null) {
                    klass = findClass(name);
                }
            } catch (ClassNotFoundException ignored) {
                // ignore
            }
            if (klass == null && getParent() != null) {
                klass = getParent().loadClass(name);
            }
            if (klass == null) {
                throw new ClassNotFoundException(name);
            }
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isNullOrEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(toClassResourceId(name));
        if (classBytesStream == null) {
            throw newClassNotFoundException(name);
        }
        byte[] classBytes = uncheckCall(() -> IOUtil.toByteArray(classBytesStream));
        definePackage(name);
        return defineClass(name, classBytes, 0, classBytes.length);
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    // argument is used in overridden implementation
    @SuppressWarnings("java:S1172")
    boolean checkShutdown(String resource) {
        return isShutdown;
    }

    InputStream resourceStream(String name) {
        if (checkShutdown(name)) {
            return null;
        }
        byte[] classData = resourcesSupplier.get().get(classKeyName(name));
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    ClassNotFoundException newClassNotFoundException(String name) {
        return new ClassNotFoundException(name);
    }

    /**
     * Defines the package if it is not already defined for the given class
     * name.
     *
     * @param className the class name
     */
    void definePackage(String className) {
        if (isNullOrEmpty(className)) {
            return;
        }
        int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return;
        }
        String packageName = className.substring(0, lastDotIndex);
        if (getDefinedPackage(packageName) != null) {
            return;
        }
        try {
            definePackage(packageName, null, null, null, null, null, null, null);
        } catch (IllegalArgumentException ignored) {
            // ignore
        }
    }
}
