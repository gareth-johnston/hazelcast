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

import com.hazelcast.config.NamespaceAwareConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A thread-local context that supplies namespace name to classloading operations.
 * Must be set around user-code serde in client messages.
 * Additionally, should be propagated via member-to-member operations.
 */
public final class NamespaceThreadLocalContext {
    private static final ILogger LOGGER = Logger.getLogger(NamespaceThreadLocalContext.class);
    private static final ThreadLocal<NamespaceThreadLocalContext> NS_THREAD_LOCAL = new ThreadLocal<>();

    private final String namespace;
    private int counter = 1;

    private NamespaceThreadLocalContext(String namespace) {
        this.namespace = namespace;
    }

    private void incCounter() {
        counter++;
    }

    private int decCounter() {
        return --counter;
    }

    @Override
    public String toString() {
        return "NamespaceThreadLocalContext{"
                + "namespace='" + namespace + '\''
                + ", counter=" + counter
                + '}';
    }

    public static void onStartNsAware(String namespace) {
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext == null) {
            tlContext = new NamespaceThreadLocalContext(namespace);
            NS_THREAD_LOCAL.set(tlContext);
            LOGGER.finest(">> start " + tlContext);
        } else {
            if (!tlContext.namespace.equals(namespace)) {
                // doesn't look like a valid state...
                throw new IllegalStateException("Attempted to start NSTLContext for namespace " + namespace
                    + " but there is an existing context " + tlContext);
            }
            tlContext.incCounter();
            LOGGER.finest(">> inc " + tlContext);
        }
    }

    public static void onCompleteNsAware(String namespace) {
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext != null) {
            if (!tlContext.namespace.equals(namespace)) {
                throw new IllegalStateException("Attempted to complete NSTLContext for namespace " + namespace
                        + " but there is an existing context " + tlContext);
            }
            int count = tlContext.decCounter();
            LOGGER.finest(">> dec " + tlContext);
            if (count == 0) {
                NS_THREAD_LOCAL.remove();
            }
        }
    }

    public static String getNamespaceThreadLocalContext() {
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext == null) {
            return NamespaceAwareConfig.DEFAULT_NAMESPACE;
        } else {
            return tlContext.namespace;
        }
    }
}
