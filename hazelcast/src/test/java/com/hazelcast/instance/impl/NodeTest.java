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
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;

public class NodeTest {

    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDDisabled() {
        Config config = new Config();
        config.setClassLoader(NodeTest.class.getClassLoader());
        ClassLoader fromNode = Node.getConfigClassloader(config);
        assertSame(config.getClassLoader(), fromNode);
    }

    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDEnabled_thenIsUCDClassLoader() {
        Config config = new Config();
        config.setClassLoader(NodeTest.class.getClassLoader());
        config.getUserCodeDeploymentConfig().setEnabled(true);
        ClassLoader fromNode = Node.getConfigClassloader(config);
        assertThat(fromNode, instanceOf(UserCodeDeploymentClassLoader.class));
        assertSame(config.getClassLoader(), fromNode.getParent());
    }

    @Test
    public void testConfigClassLoader_whenNamespaceExists_andUCDEnabled_thenIsNsAwareWithUCDParent() {
        Config config = new Config();
        config.setClassLoader(NodeTest.class.getClassLoader());
        config.getUserCodeDeploymentConfig().setEnabled(true);
        config.getNamespacesConfig().addNamespaceConfig(new NamespaceConfig("namespace"));
        ClassLoader fromNode = Node.getConfigClassloader(config);
        assertThat(fromNode, instanceOf(NamespaceAwareClassLoader.class));
        assertThat(fromNode.getParent(), instanceOf(UserCodeDeploymentClassLoader.class));
        assertSame(config.getClassLoader(), fromNode.getParent().getParent());
    }
}
