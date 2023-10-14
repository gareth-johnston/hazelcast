package com.hazelcast.instance.impl;

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;

import static org.junit.Assert.assertEquals;

// TODO Only for demo, delete after
public class UCDDemoDerbyTest extends NamespaceAwareClassLoaderIntegrationTest {
    /**
     * As a Java developer, I can define a MapLoader with JDBC driver dependency in a namespace and IMap configured with that
     * namespace will correctly instantiate and use my MapLoader.
     */
    @Test
    public void test() throws ClassNotFoundException, MalformedURLException {
        String mapName = randomMapName();
        String className = "usercodedeployment.DerbyUpperCaseStringMapLoader";

        Assert.assertThrows("The test class should not be already accessible", ClassNotFoundException.class,
                () -> Class.forName(className));

        // Create a namespace
        NamespaceConfig namespace = new NamespaceConfig("ns1");

        // Add my MapLoader
        namespace.addClass(mapResourceClassLoader.loadClass(className));

        // Add a JDBC driver (Derby)
        namespace.addJar(new URL("https://repo1.maven.org/maven2/org/apache/derby/derby/10.15.2.0/derby-10.15.2.0.jar"));
        namespace.addJar(
                new URL("https://repo1.maven.org/maven2/org/apache/derby/derbyshared/10.15.2.0/derbyshared-10.15.2.0.jar"));

        // Configure the instance
        config.addNamespaceConfig(namespace);

        // Configure the map
        config.getMapConfig(mapName).setNamespace(namespace.getName()).getMapStoreConfig().setEnabled(true)
                .setClassName(className);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        // nodeClassLoader = Node.getConfigClassloader(config);

        String input = "User Code Deployment Demo";

        System.err.println(MessageFormat.format("Querying map for input \"{0}\"...", input));
        String mapped = hazelcastInstance.<String, String> getMap(mapName).get(input);

        System.err.println(MessageFormat.format("Returned value was \"{0}\"", mapped));
        assertEquals("USER CODE DEPLOYMENT DEMO", mapped);
    }
}
