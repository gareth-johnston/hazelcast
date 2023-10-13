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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V5_4;
import static java.lang.String.format;

// RU_COMPAT_5_3 "implements Versioned" can be removed in 5.5
public class AddDynamicConfigOperation extends AbstractDynamicConfigOperation implements Versioned {

    private IdentifiedDataSerializable config;
    private int memberListVersion;
    // User Code Deployment
    private String namespace;

    public AddDynamicConfigOperation() {

    }

    public AddDynamicConfigOperation(IdentifiedDataSerializable config, int memberListVersion, @Nullable String namespace) {
        this.config = config;
        this.namespace = namespace;
        this.memberListVersion = memberListVersion;
    }

    @Override
    public void run() throws Exception {
        ClusterWideConfigurationService service = getService();
        service.registerConfigLocally(config, ConfigCheckMode.THROW_EXCEPTION);
        ClusterService clusterService = getNodeEngine().getClusterService();
        if (clusterService.isMaster()) {
            int currentMemberListVersion = clusterService.getMemberListVersion();
            if (currentMemberListVersion != memberListVersion) {
                throw new ClusterTopologyChangedException(
                        format("Current member list version %d does not match expected %d", currentMemberListVersion,
                                memberListVersion));
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(V5_4)) {
            // We need namespace first for config deser
            out.writeString(namespace);
            out.writeInt(memberListVersion);
            out.writeObject(config);
        } else {
            out.writeObject(config);
            out.writeInt(memberListVersion);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(V5_4)) {
            // We need namespace first for config deser
            namespace = in.readString();
            memberListVersion = in.readInt();
            config = NamespaceUtil.callWithNamespace(namespace, in::readObject);
        } else {
            config = in.readObject();
            memberListVersion = in.readInt();
        }
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.ADD_DYNAMIC_CONFIG_OP;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return (throwable instanceof ClusterTopologyChangedException) ? ExceptionAction.THROW_EXCEPTION
                : super.onInvocationException(throwable);
    }
}
