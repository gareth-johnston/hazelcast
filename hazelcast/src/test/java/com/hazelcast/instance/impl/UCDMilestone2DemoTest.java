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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.JobRepository.classKeyName;

// TODO Delete after demo
public class UCDMilestone2DemoTest extends HazelcastTestSupport {
    private static Path classRoot = Paths.get("src/test/class");
    protected static MapResourceClassLoader mapResourceClassLoader;
    private static String namespaceName = UCDMilestone2DemoTest.class.getSimpleName();
    private static Object KEY = Void.TYPE;
    private static String VALUE = WordUtils.capitalizeFully(StringUtils.substringAfterLast(MobyNames.getRandomName(0), "_"));
    private static PrintStream out = System.out;

    private HazelcastInstance instance;
    private String mapName;

    @BeforeClass
    public static void setUpClass() throws IOException {
        mapResourceClassLoader = generateMapResourceClassLoaderForDirectory(classRoot);
    }

    @Before
    public void setUp() {
        Config config = new Config();
        config.getNamespacesConfig().setEnabled(true);
        mapName = randomMapName();

        instance = createHazelcastInstance(config);
        instance.getConfig().getMapConfig(mapName).setNamespace(namespaceName);
    }

    @Test
    public void testMilestone2TestCase1() throws Exception {
        String className = "usercodedeployment.ModifyCaseValueEntryProcessor";

        Exception e = assertThrows(Exception.class, () -> mapWorker(className));
        out.println(MessageFormat.format("Ooops, forgot to setup the namespace! {0}", e.getMessage()));

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
        instance.getConfig().getNamespacesConfig().addNamespaceConfig(namespace);
    }

    private void mapWorker(String className) throws Exception {
        IMap<Object, String> map = instance.getMap(mapName);

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

    @Test
    public void testMilestone2TestCase2() throws Exception {
        // TODO Doesnt work
        String clazz = "usercodedeployment.H2WithDataSourceBuildVersionMapLoader";
        instance.getConfig().getMapConfig(mapName).getMapStoreConfig().setEnabled(true).setClassName(clazz);

        configureNamespace(clazz, new URL("https://repo1.maven.org/maven2/com/h2database/h2/2.0.202/h2-2.0.202.jar"));
        executeMapLoader();

        configureNamespace(clazz, new URL("https://repo1.maven.org/maven2/com/h2database/h2/2.0.204/h2-2.0.204.jar"));
        executeMapLoader();
    }

    private void configureNamespace(String className, URL h2URL) throws Exception {
        out.println(MessageFormat.format("Adding H2 from \"{0}\" to namespace...", h2URL));
        NamespaceConfig namespace =
                new NamespaceConfig(namespaceName).addClass(mapResourceClassLoader.loadClass(className)).addJar(h2URL);
        instance.getConfig().getNamespacesConfig().addNamespaceConfig(namespace);
    }

    private void executeMapLoader() {
        instance.getMap(mapName).destroy();

        IMap<String, String> map = instance.getMap(mapName);

        // Ensure MapLoader is executed
        map.loadAll(false);

        out.println(MessageFormat.format("MapLoader returned \"{0}\"", map.get(KEY)));
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

    private Class<?> tryLoadClass(String namespace, String className) throws Exception {
        if (namespace != null) {
            NamespaceThreadLocalContext.onStartNsAware(namespace);
        }
        try {
            return Accessors.getNode(instance).getConfigClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(
                    MessageFormat.format("\"{0}\" class not found in \"{1}\" namespace", className, namespace), e);
        } finally {
            if (namespace != null) {
                NamespaceThreadLocalContext.onCompleteNsAware(namespace);
            }
        }
    }
}
