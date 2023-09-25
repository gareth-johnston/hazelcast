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

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.test.HazelcastParametrizedRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(HazelcastParametrizedRunner.class)
public class NamespaceServiceImplTest {

    @Parameter(0)
    public String name;

    @Parameter(1)
    public Set<ResourceDefinition> resources;

    private NamespaceServiceImpl namespaceService;

    @Parameters(name = "Source: {0}")
    public static Iterable<Object[]> parameters() throws IOException {
        return List.of(
                new Object[] {"JAR",
                        singletonJarResourceFromClassPath("usercodedeployment/ChildParent.jar",
                                "usercodedeployment/ChildParent.jar")},
                new Object[] {"Class",
                        classResourcesFromClassPath(
                                BiTuple.of("usercodedeployment.ChildClass", "usercodedeployment/ChildClass.class"),
                                BiTuple.of("usercodedeployment.ParentClass", "usercodedeployment/ParentClass.class"))});
    }

    @Before
    public void setup() {
        namespaceService = new NamespaceServiceImpl(NamespaceServiceImplTest.class.getClassLoader());
    }

    @Test
    public void testLoadClasses() throws Exception {
        namespaceService.addNamespace("ns1", resources);
        ClassLoader classLoader = namespaceService.namespaceToClassLoader.get("ns1");
        Class<?> klass = classLoader.loadClass("usercodedeployment.ParentClass");
        klass = classLoader.loadClass("usercodedeployment.ChildClass");
        klass.getDeclaredConstructor().newInstance();
    }

    private static Set<ResourceDefinition> singletonJarResourceFromClassPath(String id, String path) throws IOException {
        try (InputStream inputStream = NamespaceServiceImplTest.class.getClassLoader().getResourceAsStream(path)) {
            return Collections.singleton(new ResourceDefinitionImpl(id, inputStream.readAllBytes(), ResourceType.JAR));
        }
    }

    @SafeVarargs
    private static Set<ResourceDefinition> classResourcesFromClassPath(BiTuple<String, String>... idPathTuples) throws IOException {
        return Arrays.stream(idPathTuples).map(idPathTuple -> {
            try {
                byte[] bytes = IOUtils
                        .toByteArray(NamespaceServiceImplTest.class.getClassLoader().getResource(idPathTuple.element2));
                return new ResourceDefinitionImpl(idPathTuple.element1, bytes, ResourceType.CLASS);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());
    }
}
