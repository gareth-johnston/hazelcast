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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.jet.impl.util.IOUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

public class NamespaceServiceImplTest {

    NamespaceServiceImpl namespaceService;

    @Before
    public void setup() {
        namespaceService = new NamespaceServiceImpl(NamespaceServiceImplTest.class.getClassLoader());
    }

    @Test
    public void testLoadClassesFromJar() throws Exception {
        namespaceService.addNamespace("ns1", singletonJarResourceFromClassPath("ChildParent.jar",
                "ChildParent.jar"));
        ClassLoader classLoader = namespaceService.namespaceToClassLoader.get("ns1");
        Class<?> klass = classLoader.loadClass("usercodedeployment.ParentClass");
        Object o = klass.getDeclaredConstructor().newInstance();
        klass = classLoader.loadClass("usercodedeployment.ChildClass");
        o = klass.getDeclaredConstructor().newInstance();
    }

    @Test
    public void testLoadClassFromClassFile() throws Exception {
        // todo need a compiled class file in test resources, doesn't look like we have one
        namespaceService.addNamespace("ns1", singletonJarResourceFromClassPath("ChildParent.jar",
                "ChildParent.jar"));
        ClassLoader classLoader = namespaceService.namespaceToClassLoader.get("ns1");
        Class<?> klass = classLoader.loadClass("usercodedeployment.ParentClass");
        Object o = klass.getDeclaredConstructor().newInstance();
        klass = classLoader.loadClass("usercodedeployment.ChildClass");
        o = klass.getDeclaredConstructor().newInstance();
    }

    Set<ResourceDefinition> singletonJarResourceFromClassPath(String id, String path) throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
        byte[] bytes = IOUtil.readFully(inputStream);
        return Collections.singleton(new ResourceDefinitionImpl(id, bytes, ResourceType.JAR));
    }
}