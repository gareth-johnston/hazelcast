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

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.UserCodeUtil.fileRelativeToBinariesFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.net.UrlEscapers;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.io.Files;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.test.HazelcastParametrizedRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
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
                        singletonJarResourceFromBinaries("usercodedeployment/ChildParent.jar",
                                "usercodedeployment/ChildParent.jar")},
                new Object[] {"Class",
                        classResourcesFromClassPath(
                                BiTuple.of("usercodedeployment.ChildClass", "usercodedeployment/ChildClass.class"),
                                BiTuple.of("usercodedeployment.ParentClass", "usercodedeployment/ParentClass.class"))});
    }

    @Before
    public void setup() {
        namespaceService = new NamespaceServiceImpl(NamespaceServiceImplTest.class.getClassLoader(), Collections.emptyMap());
    }

    @Test
    public void testLoadClasses() throws Exception {
        namespaceService.addNamespace("ns1", resources);
        final ClassLoader classLoader = namespaceService.namespaceToClassLoader.get("ns1");

        Class<?> klass = classLoader.loadClass("usercodedeployment.ParentClass");
        klass.getDeclaredConstructor().newInstance();

        klass = classLoader.loadClass("usercodedeployment.ChildClass");
        klass.getDeclaredConstructor().newInstance();
    }

    private static Set<ResourceDefinition> singletonJarResourceFromBinaries(final String id, final String path)
            throws IOException {
        final byte[] bytes = Files.toByteArray(fileRelativeToBinariesFolder(path));
        return Collections.singleton(new ResourceDefinitionImpl(id, bytes, ResourceType.JAR));
    }

    @SafeVarargs
    private static Set<ResourceDefinition> classResourcesFromClassPath(final BiTuple<String, String>... idPathTuples)
            throws IOException {
        return Arrays.stream(idPathTuples).map(idPathTuple -> {
            try {
                final byte[] bytes = Files.toByteArray(fileRelativeToBinariesFolder(idPathTuple.element2));
                return new ResourceDefinitionImpl(idPathTuple.element1, bytes, ResourceType.CLASS);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());
    }

    // TODO This test is hacky and probably does not belong here - we should refactor/move it eventually
    @Test
    public void testXmlConfigLoadingForNamespacesWithIMap() {
        Path pathToJar = Paths.get("src", "test", "class", "usercodedeployment", "ChildParent.jar");
        String stringPath = UrlEscapers.urlFragmentEscaper().escape(pathToJar.toAbsolutePath().toString());
        // Windows things
        stringPath = stringPath.replace("\\", "/");
        String xmlPayload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "           xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n"
                + "           http://www.hazelcast.com/schema/config/hazelcast-config-5.0.xsd\">\n" + "\n"
                + "    <cluster-name>cluster</cluster-name>\n\n"
                + "    <namespaces enabled=\"true\">\n" +
                "        <namespace name=\"myNamespace\">\n"
                + "          <resource type=\"JAR\">\n"
                + "              <url>file:///" + stringPath + "</url>\n"
                + "          </resource>\n"
                + "      </namespace>\n"
                + "    </namespaces>\n\n"
                + "    <map name=\"myMap\">\n"
                + "        <namespace>myNamespace</namespace>\n"
                + "    </map>\n"
                + "</hazelcast>\n" + "\n";

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(Config.loadFromString(xmlPayload));
        try {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            NamespaceService service = nodeEngine.getNamespaceService();
            assertTrue(service.isEnabled());
            assertTrue(nodeEngine.getConfigClassLoader() instanceof NamespaceAwareClassLoader);
            assertTrue(service.hasNamespace("myNamespace"));

            MapConfig mapConfig = instance.getConfig().getMapConfig("myMap");
            assertEquals("myNamespace", mapConfig.getNamespace());
        } finally {
            instance.shutdown();
        }
    }

    // "No-op" implementation test TODO Should this be in a separate test class? It would only be the 1 test...
    @Test
    public void testNoOpImplementation() {
        // Do not enable Namespaces in any form, results in No-Op implementation being used
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(smallInstanceConfig());
        try {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            NamespaceService service = nodeEngine.getNamespaceService();
            assertFalse(service.isEnabled());
            assertFalse(nodeEngine.getConfigClassLoader() instanceof NamespaceAwareClassLoader);
            assertFalse(service.isDefaultNamespaceDefined());
        } finally {
            instance.shutdown();
        }
    }
}
