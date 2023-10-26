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
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.namespace.NamespaceService.DEFAULT_NAMESPACE_ID;

/**
 * Utility to simplify accessing the NamespaceService and Namespace-aware wrapping
 */
public class NamespaceUtil {

    private NamespaceUtil() {
    }

    // TODO Docs for all methods
    public static void setupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        engine.getNamespaceService().setupNamespace(namespace);
    }

    public static void cleanupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
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
}
