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

import com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class ResourceDefinitionImpl implements ResourceDefinition {
    private final String id;
    private final byte[] payload;
    private final ResourceType type;

    public ResourceDefinitionImpl(String id, byte[] payload, ResourceType type) {
        this.id = id;
        this.payload = payload;
        this.type = type;
    }

    public ResourceDefinitionImpl(ResourceConfig resourceConfig) {
        try (InputStream is = resourceConfig.getUrl().openStream()) {
            id = resourceConfig.getId();
            payload = is.readAllBytes();
            type = resourceConfig.getResourceType();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Could not open stream for resource id " + resourceConfig.getId() + " and URL " + resourceConfig.getUrl(),
                    e);
        }
    }

    public ResourceDefinitionImpl(ResourceDefinitionHolder holder) {
        id = holder.getId();
        payload = holder.getPayload();
        type = ResourceType.getById(holder.getResourceType());
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public ResourceType type() {
        return type;
    }

    @Override
    public byte[] payload() {
        return payload;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ResourceDefinitionImpl other = (ResourceDefinitionImpl) obj;
        return Objects.equals(id, other.id);
    }
}
