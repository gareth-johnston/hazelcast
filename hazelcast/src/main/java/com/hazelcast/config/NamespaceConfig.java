package com.hazelcast.config;

import com.hazelcast.jet.config.ResourceConfig;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceConfig implements NamedConfig {

    private String name;

    private final Set<ResourceConfig> resourceConfigs = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public NamedConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    public NamespaceConfig addResourceConfig(ResourceConfig resourceConfig) {
        resourceConfigs.add(resourceConfig);
        return this;
    }

    // todo: maybe remove(String id)?
    public NamespaceConfig removeResourceConfig(ResourceConfig resourceConfig) {
        resourceConfigs.remove(resourceConfig);
        return this;
    }

    Set<ResourceConfig> getResourceConfigs() {
        return Collections.unmodifiableSet(resourceConfigs);
    }
}
