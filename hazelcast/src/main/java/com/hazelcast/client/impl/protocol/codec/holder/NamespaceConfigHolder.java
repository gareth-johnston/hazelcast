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
package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;

import java.util.Map;
import java.util.Objects;

/**
 * Holder used to support the client protocol.
 */
public final class NamespaceConfigHolder {
    private final String name;
    private final Map<String, Data> resourceDefinitions;

    public NamespaceConfigHolder(String name, Map<String, Data> resourceDefinitions) {
        this.name = name;
        this.resourceDefinitions = resourceDefinitions;
    }

    public String getName() {
        return name;
    }

    public Map<String, Data> getResourceDefinitions() {
        return resourceDefinitions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourceDefinitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        NamespaceConfigHolder other = (NamespaceConfigHolder) obj;
        return Objects.equals(name, other.name) && Objects.equals(resourceDefinitions, other.resourceDefinitions);
    }
}
