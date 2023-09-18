package com.hazelcast.internal.namespace;

import javax.annotation.Nonnull;
import java.util.Set;

public interface NamespaceService {

    void addNamespace(@Nonnull String nsName,
                      @Nonnull Set<ResourceDefinition> resources);

    /**
     *
     * @param nsName
     * @return {@code true} if {@code nsName} namespace was found and removed, otherwise {@code false}.
     */
    boolean removeNamespace(@Nonnull String nsName);
}
