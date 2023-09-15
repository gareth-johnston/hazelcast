package com.hazelcast.internal.namespace;

import java.util.Set;

public interface NamespaceService {

    void addNamespace(String nsName, Set<ResourceDefinition> resources);

    /**
     *
     * @param nsName
     * @return {@code true} if {@code nsName} namespace was found and removed, otherwise {@code false}.
     */
    boolean removeNamespace(String nsName);
}
