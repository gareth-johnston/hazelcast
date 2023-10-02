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

package com.hazelcast.instance.impl;

import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.UserCodeUtil.fileRelativeToBinariesFolder;
import static com.hazelcast.test.UserCodeUtil.urlFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.DeflaterOutputStream;

/**
 * Test static namespace configuration, resource resolution and classloading end-to-end.
 */
public class NamespaceAwareClassLoaderIntegrationTest {
    private static MapResourceClassLoader mapResourceClassLoader;
    
    private Config config;
    private ClassLoader nodeClassLoader;

    @BeforeClass
    public static void setupClass() throws IOException {
     // Find & load all .class files in the scope of this test
        final Path root = Paths.get("src/test/class");

        try (Stream<Path> stream = Files.walk(root.resolve("usercodedeployment"))) {
            final Map<String, byte[]> classNameToContent = stream
                    .filter(path -> FilenameUtils.isExtension(path.getFileName().toString(), "class"))
                    .collect(Collectors.toMap(path -> classKeyName(root.relativize(path).toString()), path -> {
                        try {
                            byte[] bytes = Files.readAllBytes(path);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            try (DeflaterOutputStream dos = new DeflaterOutputStream(baos)) {
                                dos.write(bytes);
                            }
                            return baos.toByteArray();
                        } catch (final IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }));

            mapResourceClassLoader = new MapResourceClassLoader(NamespaceAwareClassLoaderIntegrationTest.class.getClassLoader(),
                    () -> classNameToContent, true);
        }
    }    

    @Before
    public void setup() {
        config = new Config();
        config.setClassLoader(HazelcastInstance.class.getClassLoader());       
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoaded() throws Exception {
        nodeClassLoader = Node.getConfigClassloader(config);
        Class<?> klass = tryLoadClass("ns1", "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoadedWithNoNamespaceDefined() throws Exception {
        nodeClassLoader = Node.getConfigClassloader(config);
        Class<?> klass = tryLoadClass(null, "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenSimpleClassInNs_thenIsLoaded() throws Exception {
        config.addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass")));
        nodeClassLoader = Node.getConfigClassloader(config);

        Class<?> parentClass = tryLoadClass("ns1", "usercodedeployment.ParentClass");
        assertInstanceOf(MapResourceClassLoader.class, parentClass.getClassLoader());
    }

    @Test
    public void whenSimpleClassInNs_thenIsNotLoadedWithNoNamespaceDefined() throws Exception {
        config.addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass")));
        nodeClassLoader = Node.getConfigClassloader(config);

        assertThrows(ClassNotFoundException.class, () -> tryLoadClass(null, "usercodedeployment.ParentClass"));
    }

    @Test
    public void whenClassWithHierarchyInNs_thenIsLoaded() throws Exception {
        config.addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass"))
                        .addClass(mapResourceClassLoader.loadClass("usercodedeployment.ChildClass")));
        nodeClassLoader = Node.getConfigClassloader(config);

        Class<?> childClass = tryLoadClass("ns1", "usercodedeployment.ChildClass");
        assertEquals("usercodedeployment.ParentClass", childClass.getSuperclass().getName());
        // assert child & parent are loaded by same classloader
        assertSame(childClass.getClassLoader(), childClass.getSuperclass().getClassLoader());
    }

    Class<?> tryLoadClass(String namespace, String className) throws Exception {
        if (namespace != null) {
            NamespaceThreadLocalContext.onStartNsAware(namespace);
        }
        try {
            return nodeClassLoader.loadClass(className);
        } finally {
            if (namespace != null) {
                NamespaceThreadLocalContext.onCompleteNsAware(namespace);
            }
        }
    }

    @Test
    public void testThatDoesNotBelongHere() throws Exception {
        config.addNamespaceConfig(new NamespaceConfig("ns1")
                .addClass(mapResourceClassLoader.loadClass("usercodedeployment.IncrementingEntryProcessor")));
        config.getMapConfig("map-ns1").setNamespace("ns1");
        HazelcastInstance member1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance member2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap("map-ns1");
        // ensure the EntryProcessor can be deserialized on the member side
        for (int i = 0; i < 100; i++) {
            map.put(i, 1);
        }
        // use a different classloader with same config to instantiate the EntryProcessor
        NamespaceAwareClassLoader nsClassLoader = (NamespaceAwareClassLoader) Node.getConfigClassloader(config);
        @SuppressWarnings("unchecked")
        Class<? extends EntryProcessor<Integer, Integer, ?>> incrEPClass = (Class<? extends EntryProcessor<Integer, Integer, ?>>) nsClassLoader
                .loadClass("usercodedeployment.IncrementingEntryProcessor");
        EntryProcessor<Integer, Integer, ?> incrEp = incrEPClass.getDeclaredConstructor().newInstance();
        // invoke executeOnKey from client on all 100 keys
        for (int i = 0; i < 100; i++) {
            map.executeOnKey(i, incrEp);
            System.out.println(map.get(i));
        }
    }

    @Test
    public void whenLoadInnerClassKnownToParent_thenIsLoaded() throws Exception {
        config.addNamespaceConfig(new NamespaceConfig("ns1").addJar(
                urlFromFile(fileRelativeToBinariesFolder("/usercodedeployment/EntryProcessorWithAnonymousAndInner.jar"))));
        nodeClassLoader = Node.getConfigClassloader(config);

        tryLoadClass("ns1", "usercodedeployment.EntryProcessorWithAnonymousAndInner");
        tryLoadClass("ns1", "usercodedeployment.EntryProcessorWithAnonymousAndInner$Test");
    }
}
