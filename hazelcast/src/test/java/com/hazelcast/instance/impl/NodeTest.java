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

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;

// TODO These tests fail
public class NodeTest extends ConfigClassLoaderTest {
    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDDisabled() {
        populateConfigClassLoader();

        assertSame(config.getClassLoader(), nodeClassLoader);
    }

    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDEnabled_thenIsUCDClassLoader() {
        config.getUserCodeDeploymentConfig().setEnabled(true);

        populateConfigClassLoader();
        assertThat(nodeClassLoader, instanceOf(UserCodeDeploymentClassLoader.class));
        assertSame(config.getClassLoader(), nodeClassLoader.getParent());
    }

    @Test
    public void testConfigClassLoader_whenNamespaceExists_andUCDEnabled_thenIsNsAwareWithUCDParent() {
        config.getUserCodeDeploymentConfig().setEnabled(true);
        config.getNamespacesConfig().addNamespaceConfig(new NamespaceConfig("namespace"));

        populateConfigClassLoader();
        assertThat(nodeClassLoader, instanceOf(NamespaceAwareClassLoader.class));
        assertThat(nodeClassLoader.getParent(), instanceOf(UserCodeDeploymentClassLoader.class));
        assertSame(config.getClassLoader(), nodeClassLoader.getParent().getParent());
    }
}
