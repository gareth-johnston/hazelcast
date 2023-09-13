package com.hazelcast.jet.impl.deployment;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public abstract class AbstractClassLoader extends JetDelegatingClassLoader {

    // todo: consider alternative to IMap
    //  take into account potential deadlocks like https://hazelcast.atlassian.net/browse/HZ-3121
    protected final Supplier<IMap<String, byte[]>> resourcesSupplier;

    protected volatile boolean isShutdown;

    protected AbstractClassLoader(ClassLoader parent,
                               @Nonnull Supplier<IMap<String, byte[]>> resourcesSupplier) {
        super(parent);
        this.resourcesSupplier = resourcesSupplier;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isNullOrEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(toClassResourceId(name));
        if (classBytesStream == null) {
            throw new ClassNotFoundException(name + ". Add it using " + JobConfig.class.getSimpleName()
                    + " or start all members with it on classpath");
        }
        byte[] classBytes = uncheckCall(() -> IOUtil.toByteArray(classBytesStream));
        definePackage(name);
        return defineClass(name, classBytes, 0, classBytes.length);
    }

    @Override
    protected URL findResource(String name) {
        if (checkShutdown(name) || isNullOrEmpty(name) || !resourcesSupplier.get().containsKey(classKeyName(name))) {
            return null;
        }
        try {
            return createUrlForName(name);
        } catch (MalformedURLException e) {
            // this should never happen with custom URLStreamHandler
            throw new RuntimeException(e);
        }
    }

    protected abstract URL createUrlForName(String name) throws MalformedURLException;

    @Override
    protected Enumeration<URL> findResources(String name) {
        return Collections.enumeration(List.of(findResource(name)));
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    // argument is used in overridden implementation
    @SuppressWarnings("java:S1172")
    boolean checkShutdown(String resource) {
        return isShutdown;
    }

    InputStream resourceStream(String name) {
        if (checkShutdown(name)) {
            return null;
        }
        byte[] classData = resourcesSupplier.get().get(classKeyName(name));
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    /**
     * Defines the package if it is not already defined for the given class
     * name.
     *
     * @param className the class name
     */
    void definePackage(String className) {
        if (isNullOrEmpty(className)) {
            return;
        }
        int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return;
        }
        String packageName = className.substring(0, lastDotIndex);
        if (getDefinedPackage(packageName) != null) {
            return;
        }
        try {
            definePackage(packageName, null, null, null, null, null, null, null);
        } catch (IllegalArgumentException ignored) {
            // ignore
        }
    }
}
