package com.hazelcast.internal.namespace.impl;

import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.Callable;

public final class NoOpNamespaceService implements NamespaceService {
    @Override
    public void addNamespace(@Nonnull String nsName, @Nonnull Collection<ResourceDefinition> resources) {
        // No-op
    }

    @Override
    public boolean removeNamespace(@Nonnull String nsName) {
        return false;
    }

    @Override
    public boolean hasNamespace(String namespaceName) {
        return false;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public boolean isDefaultNamespaceDefined() {
        return false;
    }

    @Override
    public void setupNamespace(@Nullable String namespace) {
        // No-op
    }

    @Override
    public void cleanupNamespace(@Nullable String namespace) {
        // No-op
    }

    @Override
    public void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        runnable.run();
    }

    @Override
    public <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public MapResourceClassLoader getClassLoaderForNamespace(String namespace) {
        return null;
    }
}
