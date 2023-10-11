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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.UserCodeUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MapResourceClassLoaderTest {
    private Map<String, byte[]> classBytes = new HashMap<>();
    private MapResourceClassLoader classLoader;
    private ClassLoader parentClassLoader;

    @Before
    public void setup() throws IOException {
        parentClassLoader = this.getClass().getClassLoader();
        loadClassesFromJar("usercodedeployment/ChildParent.jar");
        loadClassesFromJar("usercodedeployment/IncrementingEntryProcessor.jar");
        loadClassesFromJar("usercodedeployment/ShadedClasses.jar");
    }

    @Test
    public void findClass_whenClassFromMap() throws Exception {
        classLoader = new MapResourceClassLoader(null, () -> classBytes, false);
        assertDoesNotThrow(
                () -> classLoader.findClass("usercodedeployment.ParentClass").getDeclaredConstructor().newInstance());
        assertDoesNotThrow(() -> classLoader.findClass("usercodedeployment.ChildClass").getDeclaredConstructor().newInstance());
    }

    @Test
    public void findClass_whenClassFromMapReferencesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        // IncrementingEntryProcessor implements EntryProcessor
        assertDoesNotThrow(() -> classLoader.findClass("usercodedeployment.IncrementingEntryProcessor").getDeclaredConstructor()
                .newInstance());
    }

    @Test
    public void loadClass_whenClassFromParentClassLoader() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        assertDoesNotThrow(() -> classLoader.loadClass("com.hazelcast.map.EntryProcessor"));
    }

    @Test
    public void loadClassChildFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        // com.hazelcast.core.HazelcastInstance loaded from ShadedClasses.jar is a concrete class with a main method
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        assertFalse(klass.isInterface());
    }

    @Test
    public void loadClassParentFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        // expect to load com.hazelcast.core.HazelcastInstance interface from the codebase
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        assertTrue(klass.isInterface());
    }

    @Test
    public void getResource_whenResolvableFromChild() {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        URL url = classLoader.getResource("usercodedeployment/ParentClass.class");
        assertEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
        assertEquals(toClassResourceId("usercodedeployment.ParentClass"), url.getFile());
    }

    @Test
    public void getResource_whenResolvableFromParent() {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        URL url = classLoader.getResource("com/hazelcast/map/IMap.class");
        assertNotNull(url);
        assertNotEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
    }

    @Test
    public void getResource_whenResolvableFromChild_andNotChildFirst() {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        URL url = classLoader.getResource("usercodedeployment/ParentClass.class");
        assertEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
        assertEquals(toClassResourceId("usercodedeployment.ParentClass"), url.getFile());
    }

    @Test
    public void findResource_whenNull() {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        URL url = classLoader.findResource(null);
        assertNull(url);
    }

    @Test
    public void findResource_whenEmpty() {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        URL url = classLoader.findResource("");
        assertNull(url);
    }

    @Test
    public void findResource_whenResolvableFromParent() throws IOException {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        URL url = classLoader.findResource("com/hazelcast/map/IMap.class");
        // findResource is meant to only search in this classloader's resources, not the parent
        assertNull(url);
    }

    private void loadClassesFromJar(String jarPath) throws IOException {
        JarInputStream inputStream = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            inputStream = getJarInputStream(jarPath);
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                String className = ClassLoaderUtil.extractClassName(entry.getName());
                if (className == null) {
                    continue;
                }
                baos.reset();
                try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
                    IOUtil.drainTo(inputStream, compressor);
                }
                inputStream.closeEntry();
                byte[] classDefinition = baos.toByteArray();
                classBytes.put(JobRepository.classKeyName(toClassResourceId(className)), classDefinition);
            } while (true);
        } finally {
            closeResource(inputStream);
        }
    }

    private JarInputStream getJarInputStream(String jarPath) throws IOException {
        File file = UserCodeUtil.fileRelativeToBinariesFolder(jarPath);
        if (file.exists()) {
            return new JarInputStream(new FileInputStream(file));
        }

        try {
            URL url = new URL(jarPath);
            return new JarInputStream(url.openStream());
        } catch (MalformedURLException e) {
            ignore(e);
        }

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(jarPath);
        if (inputStream == null) {
            throw new FileNotFoundException("File could not be found in " + jarPath + "  and resources/" + jarPath);
        }
        return new JarInputStream(inputStream);
    }
}
