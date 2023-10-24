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

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.internal.namespace.impl.ResourceDefinitionImpl;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.stream.Collectors;

// TODO proper documentation
public interface NamespaceService {
    String DEFAULT_NAMESPACE_ID = "default";

    void addNamespace(@Nonnull String nsName,
                      @Nonnull Collection<ResourceDefinition> resources);

    /**
     *
     * @param nsName
     * @return {@code true} if {@code nsName} namespace was found and removed, otherwise {@code false}.
     */
    boolean removeNamespace(@Nonnull String nsName);

    boolean hasNamespace(String namespaceName);

    /**
     * In order to fail-fast, we skip Namespace-awareness handling when
     * an object's namespace is `null` - however, if we have a default
     * Namespace defined, we should use that in these cases. To facilitate
     * failing fast with minimal overhead, we track this separately.
     *
     * @return {@code True} if a default Namespace exists, otherwise {@code False}
     */
    boolean isDefaultNamespaceDefined();

    default void addNamespaceConfig(NamespaceConfig config) {
        addNamespace(config.getName(),
                config.getResourceConfigs().stream().map(ResourceDefinitionImpl::new).collect(Collectors.toList()));
    }

    default void removeNamespaceConfig(NamespaceConfig config) {
        removeNamespace(config.getName());
    }

    MapResourceClassLoader getClassLoaderForNamespace(String namespace);
}
