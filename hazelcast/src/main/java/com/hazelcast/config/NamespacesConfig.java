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

package com.hazelcast.config;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** @since 5.4 */
public class NamespacesConfig {
    // TODO This does nothing!
    private boolean enabled;
    private final Map<String, NamespaceConfig> namespaceConfigs = new ConcurrentHashMap<>();

    public NamespacesConfig() {
    }

    public NamespacesConfig(NamespacesConfig config) {
        this.enabled = config.enabled;
        namespaceConfigs.putAll(config.getNamespaceConfigs());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NamespacesConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Adds the specified {@code namespaceConfig}, replacing any existing {@link NamespaceConfig} with the same
     * {@link NamespaceConfig#getName() name}.
     */
    public NamespacesConfig addNamespaceConfig(NamespaceConfig namespaceConfig) {
        namespaceConfigs.put(namespaceConfig.getName(), namespaceConfig);
        return this;
    }

    public NamespacesConfig removeNamespaceConfig(String namespaceName) {
        namespaceConfigs.remove(namespaceName);
        return this;
    }

    protected Map<String, NamespaceConfig> getNamespaceConfigs() {
        return Collections.unmodifiableMap(namespaceConfigs);
    }

    void setNamespaceConfigs(Map<String, NamespaceConfig> namespaceConfigs) {
        this.namespaceConfigs.clear();
        this.namespaceConfigs.putAll(namespaceConfigs);
    }
}
