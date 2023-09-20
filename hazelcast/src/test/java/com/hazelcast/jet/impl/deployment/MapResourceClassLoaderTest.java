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

import com.hazelcast.internal.nio.IOUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.jet.impl.JobRepository.CLASS_STORAGE_KEY_NAME_PREFIX;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;

public class MapResourceClassLoaderTest {
    private static final Pattern CLASS_PATTERN = Pattern.compile("(.*)\\.class$");

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
        Class<?> klass = classLoader.findClass("usercodedeployment.ParentClass");
        Object o = klass.getDeclaredConstructor().newInstance();
        klass = classLoader.findClass("usercodedeployment.ChildClass");
        o = klass.getDeclaredConstructor().newInstance();
    }

    @Test
    public void findClass_whenClassFromMapReferencesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        // IncrementingEntryProcessor implements EntryProcessor
        Class<?> klass = classLoader.findClass("usercodedeployment.IncrementingEntryProcessor");
        Object o = klass.getDeclaredConstructor().newInstance();
    }

    @Test
    public void loadClass_whenClassFromParentClassLoader() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        Class<?> klass = classLoader.loadClass("com.hazelcast.map.EntryProcessor");
    }

    @Test
    public void loadClassChildFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, true);
        // com.hazelcast.core.HazelcastInstance loaded from ShadedClasses.jar is a concrete class with a main method
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        Assert.assertFalse(klass.isInterface());
    }

    @Test
    public void loadClassParentFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(parentClassLoader, () -> classBytes, false);
        // expect to load com.hazelcast.core.HazelcastInstance interface from the codebase
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        Assert.assertTrue(klass.isInterface());
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

                String className = extractClassName(entry);
                if (className == null) {
                    continue;
                }
                baos.reset();
                OutputStream os = new DeflaterOutputStream(baos, true);
                try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
                    IOUtil.drainTo(inputStream, compressor);
                }
                inputStream.closeEntry();
                byte[] classDefinition = baos.toByteArray();
                // todo: decide key format for the map resource supplier
                classBytes.put(CLASS_STORAGE_KEY_NAME_PREFIX + toClassResourceId(className), classDefinition);
            } while (true);
        } finally {
            closeResource(inputStream);
        }
    }

    private String extractClassName(JarEntry entry) {
        String entryName = entry.getName();
        Matcher matcher = CLASS_PATTERN.matcher(entryName.replace('/', '.'));
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private JarInputStream getJarInputStream(String jarPath) throws IOException {
        File file = new File(jarPath);
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
