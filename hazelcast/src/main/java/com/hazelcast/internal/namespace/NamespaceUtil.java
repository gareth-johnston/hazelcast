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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Callable;

/**
 * Utility to simplify setup/cleanup of namespace aware classloading
 */
public class NamespaceUtil {

    private NamespaceUtil() {
    }

    public static void setupNs(@Nonnull Node node, @Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        if (node.namespacesEnabled) {
            NamespaceThreadLocalContext.onStartNsAware(namespace);
        }
    }

    public static void cleanupNs(@Nonnull Node node, @Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        if (node.namespacesEnabled) {
            NamespaceThreadLocalContext.onCompleteNsAware(namespace);
        }
    }

    public static void setupNs(@Nonnull NodeEngine nodeEngine, @Nullable String namespace) {
        setupNs(((NodeEngineImpl) nodeEngine).getNode(), namespace);
    }

    public static void cleanupNs(@Nonnull NodeEngine nodeEngine, @Nullable String namespace) {
        cleanupNs(((NodeEngineImpl) nodeEngine).getNode(), namespace);
    }

    public static void setupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onStartNsAware(namespace);
    }

    public static void cleanupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onCompleteNsAware(namespace);
    }

    public static void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        setupNs(namespace);
        try {
            runnable.run();
        } finally {
            cleanupNs(namespace);
        }
    }

     public static <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        setupNs(namespace);
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        } finally {
            cleanupNs(namespace);
        }
    }
}
