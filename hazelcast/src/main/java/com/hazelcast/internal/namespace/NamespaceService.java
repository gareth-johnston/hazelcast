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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.Callable;

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
     * Fast check method to see if the underlying implementation is
     * a no-op implementation (<b>not enabled</b>), or an actual
     * implementation (<b>enabled</b>).
     *
     * @return true if the underlying implementation is functional
     */
    boolean isEnabled();

    /**
     * In order to fail-fast, we skip Namespace-awareness handling when
     * an object's namespace is `null` - however, if we have a default
     * Namespace defined, we should use that in these cases. To facilitate
     * failing fast with minimal overhead, we track this separately.
     *
     * @return {@code True} if a default Namespace exists, otherwise {@code False}
     */
    boolean isDefaultNamespaceDefined();

    void setupNamespace(@Nullable String namespace);

    void cleanupNamespace(@Nullable String namespace);

    void runWithNamespace(@Nullable String namespace, Runnable runnable);

    <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable);

    default void addNamespaceConfig(NamespaceConfig config) {
        addNamespace(config.getName(), config.getResourceConfigs());
    }

    default void removeNamespaceConfig(NamespaceConfig config) {
        removeNamespace(config.getName());
    }

    ClassLoader getClassLoaderForNamespace(String namespace);
}
