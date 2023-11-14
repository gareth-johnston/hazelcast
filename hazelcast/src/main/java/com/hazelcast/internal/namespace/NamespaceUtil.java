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

import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Utility to simplify accessing the NamespaceService and Namespace-aware wrapping
 */
public class NamespaceUtil {

    private NamespaceUtil() {
    }

    // TODO Docs for all methods
    public static void setupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        setupNamespace(engine, namespace);
    }

    public static void cleanupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        cleanupNamespace(engine, namespace);
    }

    public static void setupNamespace(NodeEngine engine, @Nullable String namespace) {
        engine.getNamespaceService().setupNamespace(namespace);
    }

    public static void cleanupNamespace(NodeEngine engine, @Nullable String namespace) {
        engine.getNamespaceService().cleanupNamespace(namespace);
    }

    public static void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        runWithNamespace(engine, namespace, runnable);
    }

    public static void runWithNamespace(NodeEngine engine, @Nullable String namespace, Runnable runnable) {
        engine.getNamespaceService().runWithNamespace(namespace, runnable);
    }

    public static <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        return callWithNamespace(engine, namespace, callable);
    }

    public static <V> V callWithNamespace(NodeEngine engine, @Nullable String namespace, Callable<V> callable) {
        return engine.getNamespaceService().callWithNamespace(namespace, callable);
    }

    /**
     * Calls the passed {@link Callable} within the {@link ClassLoader} context
     * of the passed {@link Object}'s own {@link ClassLoader} as defined by
     * {@code Object#getClass#getClassLoader()}. The intention is that we can
     * retrieve a User Code Deployment class's {@link MapResourceClassLoader}
     * without the need for any additional references like we would need when
     * fetching using a {@code String namespace}.
     *
     * @implNote This should only be used on UCD objects, as the contract is
     * that all UCD objects are instantiated using the correct Namespace-aware
     * {@link ClassLoader}, allowing this shortcut to work. This also allows us
     * to handle client-executed UCD objects without fuss, as it will simply
     * use their local {@link ClassLoader}.
     *
     * @param ucdObject the UCD-instantiated object to retrieve the
     *                  {@link ClassLoader} from for execution
     * @param callable  the {@link Callable} to execute with Namespace awareness
     */
    public static <V> V callWithOwnClassLoader(Object ucdObject, Callable<V> callable) {
        ClassLoader loader = ucdObject.getClass().getClassLoader();
        NamespaceThreadLocalContext.onStartNsAware(loader);
        try {
            return callable.call();
        } catch (Exception exception) {
            throw sneakyThrow(exception);
        } finally {
            NamespaceThreadLocalContext.onCompleteNsAware(loader);
        }
    }

    /**
     * Runs the passed {@link Runnable} within the {@link ClassLoader} context
     * of the passed {@link Object}'s own {@link ClassLoader} as defined by
     * {@code Object#getClass#getClassLoader()}. The intention is that we can
     * retrieve a User Code Deployment class's {@link MapResourceClassLoader}
     * without the need for any additional references like we would need when
     * fetching with only a {@code String namespace}.
     *
     * @implNote This should only be used on UCD objects, as the contract is
     * that all UCD objects are instantiated using the correct Namespace-aware
     * {@link ClassLoader}, allowing this shortcut to work. This also allows us
     * to handle client-executed UCD objects without fuss, as it will simply
     * use their local {@link ClassLoader}.
     *
     * @param ucdObject the UCD-instantiated object to retrieve the
     *                  {@link ClassLoader} from for execution
     * @param runnable  the {@link Runnable} to execute with Namespace awareness
     */
    public static void runWithOwnClassLoader(Object ucdObject, Runnable runnable) {
        ClassLoader loader = ucdObject.getClass().getClassLoader();
        NamespaceThreadLocalContext.onStartNsAware(loader);
        try {
            runnable.run();
        } catch (Exception exception) {
            throw sneakyThrow(exception);
        } finally {
            NamespaceThreadLocalContext.onCompleteNsAware(loader);
        }
    }

    // Use namespace ClassLoader if it exists, otherwise fallback to config class loader
    public static ClassLoader getClassLoaderForNamespace(NodeEngine engine, @Nullable String namespace) {
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(namespace);
        return loader != null ? loader : getDefaultClassloader(engine);
    }

    // Use namespace CL if exists, otherwise fallback to config class loader
    public static ClassLoader getClassLoaderForNamespace(NodeEngine engine, @Nullable String namespace,
                                                         ClassLoader defaultLoader) {
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(namespace);
        return loader != null ? loader : defaultLoader;
    }

    // Use default namespace CL if exists, otherwise fallback to config class loader
    public static ClassLoader getDefaultClassloader(NodeEngine engine) {
        // Call with `null` namespace, which will fallback to a default Namespace if available
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(null);
        return loader != null ? loader : engine.getConfigClassLoader();
    }
}
