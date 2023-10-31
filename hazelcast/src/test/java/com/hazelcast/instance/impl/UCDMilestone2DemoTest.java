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

import com.google.common.base.CaseFormat;
import com.hazelcast.config.Config;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static org.junit.Assert.assertEquals;

// TODO Delete after demo
public class UCDMilestone2DemoTest extends HazelcastTestSupport {
    private static Path classRoot = Paths.get("src/test/class");
    protected static MapResourceClassLoader mapResourceClassLoader;
    private static String namespaceName = UCDMilestone2DemoTest.class.getSimpleName();
    private static Object KEY = Void.TYPE;
    private static String VALUE = WordUtils.capitalizeFully(StringUtils.substringAfterLast(MobyNames.getRandomName(0), "_"));
    private static PrintStream out = System.out;

    protected Config config;
    private ClassLoader nodeClassLoader;
    private HazelcastInstance hazelcastInstance;
    private String mapName;

    @BeforeClass
    public static void setUpClass() throws IOException {
        mapResourceClassLoader = generateMapResourceClassLoaderForDirectory(classRoot);
    }

    @Before
    public void setUp() {
        config = new Config();
        config.getNamespacesConfig().setEnabled(true);
        mapName = randomMapName();
    }

    /**
     * "As a Java developer, I can dynamically configure namespaces and their resources. A new data structure config I add at
     * runtime, can reference a namespace I dynamically added and resources will be looked up in the configured namespace."
     */
    @Test
    public void testMilestone2TestCase1() throws Exception {
        hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Accessors.getNode(hazelcastInstance).getConfigClassLoader();

        String className = "usercodedeployment.ModifyCaseValueEntryProcessor";

        Exception e = assertThrows(Exception.class, () -> mapWorker(className));
        out.println(MessageFormat.format("Ooops, forgot to setup the namespace! {0}", e.getMessage()));

        hazelcastInstance.getConfig().getMapConfig(mapName).setNamespace(namespaceName);

        configureNamespace(className, "UpperCaseValueEntryProcessor");
        mapWorker(className);

        configureNamespace(className, "LowerCaseValueEntryProcessor");
        mapWorker(className);
    }

    private void configureNamespace(String className, String directory) throws Exception {
        out.println(MessageFormat.format("Adding class in \"{0}\" to namespace", directory));
        NamespaceConfig namespace = new NamespaceConfig(namespaceName);
        namespace
                .addClass(generateMapResourceClassLoaderForDirectory(classRoot.resolve("usercodedeployment").resolve(directory))
                        .loadClass(className));
        hazelcastInstance.getConfig().getNamespacesConfig().addNamespaceConfig(namespace);
    }

    private void mapWorker(String className) throws Exception {
        IMap<Object, String> map = hazelcastInstance.getMap(mapName);

        map.put(KEY, VALUE);

        out.println();
        out.println("Executing EntryProcessor...");
        out.println(MessageFormat.format("VALUE pre-EntryProcessor was \"{0}\"", map.get(KEY)));

        Class<? extends EntryProcessor<Object, String, String>> clazz =
                (Class<? extends EntryProcessor<Object, String, String>>) tryLoadClass(namespaceName, className);
        map.executeOnKey(Void.TYPE, clazz.getDeclaredConstructor().newInstance());

        out.println(MessageFormat.format("VALUE post-EntryProcessor was \"{0}\"", map.get(KEY)));
        out.println();
    }

    /**
     * "As a Java developer, I can replace the resources in a namespace at runtime and they will be picked up the next time a
     * user customization is instantiated. Can be tested e.g. with an EntryProcessor that is executed with on implementation,
     * then namespace is updated with a new implementation and execute the EntryProcessor again to observe updated behaviour."
     *
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3413">HZ-3413 - Test cases for Milestone 2</a>
     */
    @Test
    public void testMilestone2TestCase2() {
        CaseValueProcessor processor = CaseValueProcessor.LOWER_CASE_VALUE_ENTRY_PROCESSOR;
        CaseValueProcessor otherProcessor = CaseValueProcessor.UPPER_CASE_VALUE_ENTRY_PROCESSOR;

        processor.addNamespaceToConfig(config);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Accessors.getNode(hazelcastInstance).getConfigClassLoader();

        // Assert the basic functionality
        processor.createExecuteAssertOnMap(this, hazelcastInstance);

        // Now swap the class in the namespace
        String namespaceName = processor.namespace.getName();
        hazelcastInstance.getConfig().getNamespacesConfig()
                .addNamespaceConfig(new NamespaceConfig(namespaceName).addClass(otherProcessor.clazz));

        // Now assert the behavior has swapped, too
        otherProcessor.createExecuteAssertOnMap(namespaceName, processor.mapName, this, hazelcastInstance);
    }

    /** Find & load all {@code .class} files in the scope of this test */
    private static MapResourceClassLoader generateMapResourceClassLoaderForDirectory(Path root) throws IOException {
        try (Stream<Path> stream = Files.walk(root.resolve("usercodedeployment"))) {
            final Map<String, byte[]> classNameToContent =
                    stream.filter(path -> FilenameUtils.isExtension(path.getFileName().toString(), "class"))
                            .collect(Collectors.toMap(path -> correctResourcePath(root, path), path -> {
                                try {
                                    return IOUtil.compress(Files.readAllBytes(path));
                                } catch (final IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }));

            return new MapResourceClassLoader(UCDMilestone2DemoTest.class.getClassLoader(), () -> classNameToContent, true);
        }
    }

    private static String correctResourcePath(Path root, Path path) {
        String classKeyName = classKeyName(root.relativize(path).toString());
        return OsHelper.ensureUnixSeparators(classKeyName);
    }

    Class<?> tryLoadClass(String namespace, String className) throws Exception {
        if (namespace != null) {
            NamespaceThreadLocalContext.onStartNsAware(namespace);
        }
        try {
            return nodeClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(
                    MessageFormat.format("\"{0}\" class not found in \"{1}\" namespace", className, namespace), e);
        } finally {
            if (namespace != null) {
                NamespaceThreadLocalContext.onCompleteNsAware(namespace);
            }
        }
    }

    /**
     * References to {@link EntryProcessor}s - not on the classpath - that modify the case of a {@link String} value in a map
     */
    private enum CaseValueProcessor {
        UPPER_CASE_VALUE_ENTRY_PROCESSOR(String::toUpperCase), LOWER_CASE_VALUE_ENTRY_PROCESSOR(String::toLowerCase);

        /** Use the same class name to assert isolation */
        private static final String className = "usercodedeployment.ModifyCaseValueEntryProcessor";
        private static final Object KEY = Void.TYPE;
        private static final String VALUE = "VaLuE";

        /** The operation we expect the {@link EntryProcessor} to perform - for validation purposes */
        private final UnaryOperator<String> expectedOperation;
        private final Class<? extends EntryProcessor<Object, String, String>> clazz;
        private final NamespaceConfig namespace;
        private final String mapName = randomMapName();

        /** @param expectedOperation {@link #expectedOperation} */
        CaseValueProcessor(UnaryOperator<String> expectedOperation) {
            this.expectedOperation = expectedOperation;

            try {
                clazz = (Class<? extends EntryProcessor<Object, String, String>>) generateMapResourceClassLoaderForDirectory(
                        classRoot.resolve("usercodedeployment").resolve(toString())).loadClass(className);
                namespace = new NamespaceConfig(toString()).addClass(clazz);
            } catch (ClassNotFoundException | IOException e) {
                throw new ExceptionInInitializerError(e);
            }

            assertClassNotAccessible(className);
        }

        private void addNamespaceToConfig(Config config) {
            config.getNamespacesConfig().addNamespaceConfig(namespace);
            config.getMapConfig(mapName).setNamespace(toString());
        }

        private IMap<Object, String> createExecuteAssertOnMap(UCDMilestone2DemoTest instance,
                HazelcastInstance hazelcastInstance) {
            return createExecuteAssertOnMap(toString(), mapName, instance, hazelcastInstance);
        }

        private IMap<Object, String> createExecuteAssertOnMap(String namespace, String mapName, UCDMilestone2DemoTest instance,
                HazelcastInstance hazelcastInstance) {
            // Create a map
            IMap<Object, String> map = hazelcastInstance.getMap(mapName);
            map.put(KEY, VALUE);

            try {
                // Execute the EntryProcessor
                Class<? extends EntryProcessor<Object, String, String>> clazz =
                        (Class<? extends EntryProcessor<Object, String, String>>) instance.tryLoadClass(namespace, className);
                map.executeOnKey(Void.TYPE, clazz.getDeclaredConstructor().newInstance());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            assertEquals(expectedOperation.apply(VALUE), map.get(KEY));

            return map;
        }

        @Override
        public String toString() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name());
        }
    }

    private static void assertClassNotAccessible(String className) {
        Assert.assertThrows("The test class should not be already accessible: " + className, ClassNotFoundException.class,
                () -> Class.forName(className));
    }
}
