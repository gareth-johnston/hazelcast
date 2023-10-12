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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.nio.Connection;

import java.util.concurrent.CompletableFuture;

// todo: docs, better name?
//  alternative to this extra layer of abstraction could be to create an interface
//  which has `String getNamespace()` which each inheritor would provide; then within
//  high level task (AbstractAsyncMessageTask) we check instanceof that interface
//  and handle from there; downside is providing `getNamespace()` in all inheritors
//  and *tiny* overhead of instanceof checks
abstract class AbstractNsAwareMapPartitionMessageTask<P> extends AbstractMapPartitionMessageTask<P> {

    AbstractNsAwareMapPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        onStartNsAwareSection();
        try {
            return super.processInternal();
        } finally {
            onCompleteNsAwareSection();
        }
    }

    protected void onStartNsAwareSection() {
        NamespaceUtil.setupNs(nodeEngine, getMapServiceContext().getExistingMapContainer(getDistributedObjectName()));
    }

    protected void onCompleteNsAwareSection() {
        NamespaceUtil.cleanupNs(nodeEngine, getMapServiceContext().getExistingMapContainer(getDistributedObjectName()));
    }
}
