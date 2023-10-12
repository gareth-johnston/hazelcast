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

import com.hazelcast.spi.impl.NodeEngine;

// todo replace this with something better - surely all threads are under the same NodeEngine anyway? (in member context)
// todo should this be part of NamespaceThreadLocalContext if we keep it?
// todo docs
public final class NodeEngineThreadLocalContext {

    private static final ThreadLocal<NodeEngineThreadLocalContext> NE_THREAD_LOCAL = new ThreadLocal<>();
    private final NodeEngine nodeEngine;
    private int counter = 1;

    private NodeEngineThreadLocalContext(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    private void incCounter() {
        counter++;
    }

    private int decCounter() {
        return --counter;
    }

    @Override
    public String toString() {
        return "NodeEngineThreadLocalContext{"
                + "nodeEngine='" + nodeEngine.getThisAddress() + '\''
                + ", counter=" + counter
                + '}';
    }

    public static void declareNodeEngine(NodeEngine nodeEngine) {
        NodeEngineThreadLocalContext tlContext = NE_THREAD_LOCAL.get();
        if (tlContext == null) {
            tlContext = new NodeEngineThreadLocalContext(nodeEngine);
            NE_THREAD_LOCAL.set(tlContext);
            System.out.println(">> start " + tlContext);
        } else {
            if (!tlContext.nodeEngine.equals(nodeEngine)) {
                // doesn't look like a valid state...
                throw new IllegalStateException("Attempted to start NETLContext for nodeEngine " + nodeEngine.getThisAddress()
                    + " but there is an existing context " + tlContext);
            }
            tlContext.incCounter();
            System.out.println(">> inc " + tlContext);
        }
    }

    public static void rescindNodeEngine(NodeEngine nodeEngine) {
        NodeEngineThreadLocalContext tlContext = NE_THREAD_LOCAL.get();
        if (tlContext != null) {
            if (!tlContext.nodeEngine.equals(nodeEngine)) {
                throw new IllegalStateException("Attempted to complete NETLContext for nodeEngine " + nodeEngine
                        + " but there is an existing context " + tlContext);
            }
            int count = tlContext.decCounter();
            System.out.println(">> dec " + tlContext);
            if (count == 0) {
                NE_THREAD_LOCAL.remove();
            }
        }
    }

    public static NodeEngine getNamespaceThreadLocalContext() {
        NodeEngineThreadLocalContext tlContext = NE_THREAD_LOCAL.get();
        if (tlContext == null) {
            return null;
        } else {
            return tlContext.nodeEngine;
        }
    }
}
