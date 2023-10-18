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

// todo should this be part of NamespaceThreadLocalContext if we keep it?
// todo docs
public final class NodeEngineThreadLocalContext {

    private static final ThreadLocal<NodeEngineThreadLocalContext> NE_THREAD_LOCAL = new ThreadLocal<>();
    private final NodeEngine nodeEngine;

    private NodeEngineThreadLocalContext(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public String toString() {
        return "NodeEngineThreadLocalContext{"
                + "nodeEngine='" + nodeEngine.getThisAddress() + '\''
                + '}';
    }

    public static void declareNodeEngineReference(NodeEngine nodeEngine) {
        NE_THREAD_LOCAL.set(new NodeEngineThreadLocalContext(nodeEngine));
    }

    public static void destroyNodeEngineReference() {
        NE_THREAD_LOCAL.remove();
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
