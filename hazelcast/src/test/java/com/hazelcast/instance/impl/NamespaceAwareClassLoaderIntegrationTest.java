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

import com.hazelcast.config.Config;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/**
 * Test static namespace configuration, resource resolution and classloading end-to-end.
 */
public class NamespaceAwareClassLoaderIntegrationTest {

    private Config config;
    private ClassLoader nodeClassLoader;

    @Before
    public void setup() {
        config = new Config();
        config.setClassLoader(HazelcastInstance.class.getClassLoader());
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoaded() throws Exception {
        nodeClassLoader = Node.getConfigClassloader(config);
        Class klass = tryLoadClass("ns1", "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoadedWithNoNamespaceDefined() throws Exception {
        nodeClassLoader = Node.getConfigClassloader(config);
        Class klass = tryLoadClass(null, "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenSimpleClassInNs_thenIsLoaded() throws Exception {
        config.addNamespaceConfig(new NamespaceConfig("ns1")
                .addClass("usercodedeployment.ParentClass",
                        new File("src/test/resources/usercodedeployment/ParentClass.class")));
        nodeClassLoader = Node.getConfigClassloader(config);

        tryLoadClass("ns1", "usercodedeployment.ParentClass");
    }

    @Test
    public void whenSimpleClassInNs_thenIsNotLoadedWithNoNamespaceDefined() {
        config.addNamespaceConfig(new NamespaceConfig("ns1")
                .addClass("usercodedeployment.ParentClass",
                        new File("src/test/resources/usercodedeployment/ParentClass.class")));
        nodeClassLoader = Node.getConfigClassloader(config);

        assertThrows(ClassNotFoundException.class, () ->
                tryLoadClass(null, "usercodedeployment.ParentClass"));
    }

    @Test
    public void whenClassWithHierarchyInNs_thenIsLoaded() throws Exception {
        config.addNamespaceConfig(new NamespaceConfig("ns1")
                .addClass("usercodedeployment.ParentClass",
                        new File("src/test/resources/usercodedeployment/ParentClass.class"))
                .addClass("usercodedeployment.ChildClass",
                        new File("src/test/resources/usercodedeployment/ChildClass.class")));
        nodeClassLoader = Node.getConfigClassloader(config);

        Class<?> childClass = tryLoadClass("ns1", "usercodedeployment.ChildClass");
        assertEquals("usercodedeployment.ParentClass", childClass.getSuperclass().getName());
        // assert child & parent are loaded by same classloader
        assertSame(childClass.getClassLoader(), childClass.getSuperclass().getClassLoader());
    }

    Class<?> tryLoadClass(String namespace, String className) throws Exception {
        if (namespace != null) {
            NamespaceAwareClassLoader.NAMESPACE_AWARE.set("ns1");
        }
        try {
            return nodeClassLoader.loadClass(className);
        } finally {
            NamespaceAwareClassLoader.NAMESPACE_AWARE.remove();
        }
    }
}
