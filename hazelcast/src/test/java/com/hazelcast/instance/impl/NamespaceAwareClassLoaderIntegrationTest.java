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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.MavenInterface;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/** Test static namespace configuration, resource resolution and classloading end-to-end */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class NamespaceAwareClassLoaderIntegrationTest extends HazelcastTestSupport {
    private static Path classRoot;
    protected static MapResourceClassLoader mapResourceClassLoader;
    private static Artifact h2V202Artifact;

    protected Config config;
    private ClassLoader nodeClassLoader;

    @BeforeClass
    public static void setUpClass() throws IOException {
        System.setProperty(MapResourceClassLoader.DEBUG_OUTPUT_PROPERTY, "true");
        classRoot = Paths.get("src/test/class");
        mapResourceClassLoader = generateMapResourceClassLoaderForDirectory(classRoot);
        h2V202Artifact = new DefaultArtifact("com.h2database", "h2", null, "2.0.202");
    }

    @AfterClass
    public static void cleanUpClass() {
        System.clearProperty(MapResourceClassLoader.DEBUG_OUTPUT_PROPERTY);
    }

    @Before
    public void setUp() {
        config = new Config();
        config.setClassLoader(HazelcastInstance.class.getClassLoader());
    }

    /**
     * Asserts a basic user workflow:
     * <ol>
     * <li>Add classes to the {@link NamespaceConfig}
     * <li>Assert that those classes aren't accessible by default
     * <li>Configure the a {@link HazelcastInstance} with isolated {@link IMap}s using those classes
     * <li>Assert that those classes are isolated - even with overlapping names, the correct class is used
     * <ol>
     *
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3301">HZ-3301 - Test case for Milestone 1 use cases</a>
     */
    @Test
    public void testMilestone1() throws Exception {
        // "I can statically configure a namespace with a java class that gets resolved at runtime"
        for (CaseValueProcessor processor : CaseValueProcessor.values()) {
            processor.addNamespaceToConfig(config);
        }

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Node.getConfigClassloader(config);

        // "I can run a customer entry processor and configure an IMap in that namespace"
        // "to execute that entry processor on that IMap"
        // "I can configure N > 1 namespaces with simple Java class resources of same name and different behavior"
        for (CaseValueProcessor processor : CaseValueProcessor.values()) {
            processor.createExecuteAssertOnMap(this, hazelcastInstance);
        }

        // "IMaps configured in the respective namespaces will correctly load and execute the respective EntryProcessor defined
        // in their namespace, without class name clashes."
        for (CaseValueProcessor processor : CaseValueProcessor.values()) {
            processor.assertEntryUpdated();
        }
    }

    /**
     * Asserts a basic user workflow:
     * <ol>
     * <li>Creates a {@link NamespaceConfig} referencing a {@link MapLoader} {@code .class} and it's database dependency - which
     * *aren't* on the classpath
     * <li>Create a map, configuring the {@link NamespaceConfig} and {@link MapLoader}
     * <li>Assert the {@link MapLoader} has loaded data via the database dependency via some dummy call
     * <ol>
     *
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3357">HZ-3357 - Test case for Milestone 1 dependencies use
     *      cases</a>
     */
    @Test
    public void testMilestone1Dependencies() throws Exception {
        // "As a Java developer, I can define a MapLoader with JDBC driver dependency in a namespace and IMap configured with
        // that namespace will correctly instantiate and use my MapLoader."

        String mapName = randomMapName();
        String className = "usercodedeployment.DerbyUpperCaseStringMapLoader";

        Assert.assertThrows("The test class should not be already accessible", ClassNotFoundException.class,
                () -> Class.forName(className));

        // Add the latest Derby version that supports Java 11 (newer versions require Java 17)
        NamespaceConfig namespace = new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass(className))
                .addJar(MavenInterface.locateArtifact(new DefaultArtifact("org.apache.derby", "derby", null, "10.15.2.0"))
                        .toUri().toURL())
                .addJar(MavenInterface.locateArtifact(new DefaultArtifact("org.apache.derby", "derbyshared", null, "10.15.2.0"))
                        .toUri().toURL());

        config.addNamespaceConfig(namespace);
        config.getMapConfig(mapName).setNamespace(namespace.getName()).getMapStoreConfig().setEnabled(true)
                .setClassName(className);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Node.getConfigClassloader(config);

        String mapped = executeMapLoader(hazelcastInstance, mapName);
        assertNotNull("Was the MapStore executed?", mapped);
        assertEquals(getClass().getSimpleName().toUpperCase(), mapped);
    }

    /**
     * Asserts a basic user workflow:
     * <ol>
     * <li>Creates a {@link NamespaceConfig} referencing a {@link MapLoader} {@code .class} and it's database dependency - which
     * is a *different* one than on classpath
     * <li>Create a map, configuring the {@link NamespaceConfig} and {@link MapLoader}
     * <li>Assert the {@link MapLoader} has used the {@link NamespaceConfig} database dependency version
     * <ol>
     *
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3357">HZ-3357 - Test case for Milestone 1 dependencies use
     *      cases</a>
     */
    @Test
    public void testMilestone1DependenciesIsolation() throws Exception {
        // "Isolation against Hazelcast member classpath: even when Hazelcast member classpath includes a clashing version of my
        // JDBC driver, my preferred JDBC driver version that is configured in namespace resources is used by my MapLoader."

        String mapName = randomMapName();
        String className = "usercodedeployment.H2WithDriverManagerBuildVersionMapLoader";

        Assert.assertThrows("The test class should not be already accessible", ClassNotFoundException.class,
                () -> Class.forName(className));

        // Deliberately use an older version
        NamespaceConfig namespace = new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass(className))
                .addJar(MavenInterface.locateArtifact(h2V202Artifact).toUri().toURL());
        config.addNamespaceConfig(namespace);

        config.getMapConfig(mapName).setNamespace(namespace.getName()).getMapStoreConfig().setEnabled(true)
                .setClassName(className);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Node.getConfigClassloader(config);

        String namespaceH2Version = executeMapLoader(hazelcastInstance, mapName);

        assertNotNull("Was the MapStore executed?", namespaceH2Version);
        assertEquals("Unexpected version of H2 found in namespace", h2V202Artifact.getVersion(), namespaceH2Version);
        assertNotEquals("Namespaces dependencies do not appear to be isolated", org.h2.engine.Constants.VERSION,
                namespaceH2Version);
    }

    @Test
    public void testThatDoesNotBelongHere() throws Exception {
        int nodeCount = 2;
        TestHazelcastFactory factory = new TestHazelcastFactory(nodeCount);

        try {
            config.addNamespaceConfig(new NamespaceConfig("ns1")
                    .addClass(mapResourceClassLoader.loadClass("usercodedeployment.IncrementingEntryProcessor")));
            config.getMapConfig("map-ns1").setNamespace("ns1");

            for (int i = 0; i < nodeCount; i++) {
                factory.newHazelcastInstance(config);
            }

            HazelcastInstance client = factory.newHazelcastClient();
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
        } finally {
            factory.terminateAll();
        }
    }

    /**
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3390">HZ-3390 - Support ServiceLoader in
     *      NamespaceAwareClassLoader</a>
     */
    @Test
    public void testServiceLoader() throws Exception {
        // Class + Map Name
        Pair<String, String> driverManager = Pair.of("usercodedeployment.H2WithDriverManagerBuildVersionMapLoader",
                randomMapName());
        Pair<String, String> dataSource = Pair.of("usercodedeployment.H2WithDataSourceBuildVersionMapLoader", randomMapName());

        Pair<String, String>[] classes = new Pair[] {driverManager, dataSource};

        NamespaceConfig namespace = new NamespaceConfig("ns1")
                .addJar(MavenInterface.locateArtifact(h2V202Artifact).toUri().toURL());
        config.addNamespaceConfig(namespace);

        for (Pair<String, String> clazz : classes) {
            namespace.addClass(mapResourceClassLoader.loadClass(clazz.getLeft()));
            config.getMapConfig(clazz.getRight()).setNamespace(namespace.getName()).getMapStoreConfig().setEnabled(true)
                    .setClassName(clazz.getLeft());
        }

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        nodeClassLoader = Node.getConfigClassloader(config);

        assertEquals("Fixture setup of JDBC with explicit driver declaration", h2V202Artifact.getVersion(),
                executeMapLoader(hazelcastInstance, dataSource.getRight()));

        assertEquals("JDBC generally is working, but Driver Manager isn't - suggests Service Loader issue",
                h2V202Artifact.getVersion(), executeMapLoader(hazelcastInstance, driverManager.getRight()));
    }

    /** Find & load all .class files in the scope of this test */
    private static MapResourceClassLoader generateMapResourceClassLoaderForDirectory(Path root) throws IOException {
        try (Stream<Path> stream = Files.walk(root.resolve("usercodedeployment"))) {
            final Map<String, byte[]> classNameToContent = stream
                    .filter(path -> FilenameUtils.isExtension(path.getFileName().toString(), "class"))
                    .collect(Collectors.toMap(path -> classKeyName(root.relativize(path).toString()), path -> {
                        try {
                            return IOUtil.compress(Files.readAllBytes(path));
                        } catch (final IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }));

            return new MapResourceClassLoader(NamespaceAwareClassLoaderIntegrationTest.class.getClassLoader(),
                    () -> classNameToContent, true);
        }
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

    private String executeMapLoader(HazelcastInstance hazelcastInstance, String mapName) {
        return hazelcastInstance.<String, String>getMap(mapName).get(getClass().getSimpleName());
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
        private final NamespaceConfig namespace;
        private final String mapName = randomMapName();
        private IMap<Object, String> map;

        /** @param expectedOperation {@link #expectedOperation} */
        CaseValueProcessor(UnaryOperator<String> expectedOperation) {
            this.expectedOperation = expectedOperation;

            try {
                namespace = new NamespaceConfig(toString()).addClass(
                        generateMapResourceClassLoaderForDirectory(classRoot.resolve("usercodedeployment").resolve(toString()))
                                .loadClass(className));
            } catch (ClassNotFoundException | IOException e) {
                throw new ExceptionInInitializerError(e);
            }

            Assert.assertThrows("The test class should not be already accessible", ClassNotFoundException.class,
                    () -> Class.forName(className));
        }

        private void addNamespaceToConfig(Config config) {
            config.addNamespaceConfig(namespace);
            config.getMapConfig(mapName).setNamespace(toString());
        }

        private void createExecuteAssertOnMap(NamespaceAwareClassLoaderIntegrationTest instance,
                HazelcastInstance hazelcastInstance) throws Exception {
            // Create a map
            map = hazelcastInstance.getMap(mapName);
            map.put(KEY, VALUE);

            // Execute the EntryProcessor
            Class<? extends EntryProcessor<Object, String, String>> clazz = (Class<? extends EntryProcessor<Object, String, String>>) instance
                    .tryLoadClass(toString(), className);
            map.executeOnKey(Void.TYPE, clazz.getDeclaredConstructor().newInstance());

            assertEntryUpdated();
        }

        private void assertEntryUpdated() {
            assertEquals(expectedOperation.apply(VALUE), map.get(KEY));
        }

        @Override
        public String toString() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name());
        }
    }
}
