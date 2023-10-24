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
 * Utility to simplify setup/cleanup of namespace aware classloading
 */
// TODO: When we want a feature flag for NS-UCD, we will handle everything via the service
//      and use a no-op implementation - this utility needs to be replaced or use that service
public class NamespaceUtil {

    private NamespaceUtil() {
    }

    // Private method to avoid calling without NodeEngine enablement checks first
    private static void setupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onStartNsAware(namespace);
    }

    // Private method to avoid calling without NodeEngine enablement checks first
    private static void cleanupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onCompleteNsAware(namespace);
    }

    public static void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        runWithNamespace(engine, namespace, runnable);
    }

    public static void runWithNamespace(NodeEngine engine, @Nullable String namespace, Runnable runnable) {
        if (!isNamespacesEnabled(engine)) {
            runnable.run();
            return;
        }

        namespace = transformNamespace(engine, namespace);
        setupNs(namespace);
        try {
            runnable.run();
        } finally {
            cleanupNs(namespace);
        }
    }

    public static <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNamespaceThreadLocalContext();
        return callWithNamespace(engine, namespace, callable);
    }

    public static <V> V callWithNamespace(NodeEngine engine, @Nullable String namespace, Callable<V> callable) {
        if (!isNamespacesEnabled(engine)) {
            try {
                return callable.call();
            } catch (Exception e) {
                throw ExceptionUtil.sneakyThrow(e);
            }
        }

        namespace = transformNamespace(engine, namespace);
        setupNs(namespace);
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        } finally {
            cleanupNs(namespace);
        }
    }

    // Internal method to transform a `null` namespace into the default namespace if available
    public static String transformNamespace(NodeEngine engine, String namespace) {
        if (namespace != null) {
            return namespace;
        } else if (engine.getNamespaceService() != null && engine.getNamespaceService().isDefaultNamespaceDefined()) {
            // Check if we have a `default` environment available
            return DEFAULT_NAMESPACE_ID;
        } else {
            // Namespace is null, no default Namespace is defined, fail-fast
            return null;
        }
    }

    // TODO usage will be replaced by no-op service
    private static boolean isNamespacesEnabled(NodeEngine engine) {
        return ((NodeEngineImpl) engine).getNode().namespacesEnabled;
    }
}
