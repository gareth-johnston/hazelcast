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

package com.hazelcast.test.starter;

import static com.hazelcast.internal.util.Preconditions.checkState;

import org.apache.commons.io.FilenameUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.internal.impl.DefaultLocalRepositoryProvider;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.LocalRepositoryManager;
import org.eclipse.aether.repository.NoLocalRepositoryManagerException;
import org.h2.util.StringUtils;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.version.Version;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HazelcastVersionLocator {
    private static final String GROUP_ID = "com.hazelcast";

    public enum Artifact {
        OS_JAR(false, false, "hazelcast", "OS"),
        OS_TEST_JAR(false, true, "hazelcast", "OS tests"),
        SQL_JAR(false, false, "hazelcast-sql", "SQL"),
        EE_JAR(true, false, "hazelcast-enterprise", "EE");

        private final boolean enterprise;
        private final boolean test;
        private final String artifactId;
        private final String label;

        Artifact(final boolean enterprise, final boolean test, final String artifactId, final String label) {
            this.enterprise = enterprise;
            this.test = test;
            this.artifactId = artifactId;
            this.label = label;
        }

        private org.eclipse.aether.artifact.Artifact toAetherArtifact(final String version) {
            return new DefaultArtifact(GROUP_ID, artifactId, test ? "tests" : null, null, version);
        }

        @Override
        public String toString() {
            return label;
        }
    }

    public static Map<Artifact, File> locateVersion(final String version, final boolean enterprise) {
        final Stream.Builder<Artifact> files = Stream.builder();
        files.add(Artifact.OS_JAR);
        files.add(Artifact.OS_TEST_JAR);
        if (Version.of(version).isGreaterOrEqual(Versions.V5_0)) {
            files.add(Artifact.SQL_JAR);
        }
        if (enterprise) {
            files.add(Artifact.EE_JAR);
        }
        return files.build().collect(Collectors.toMap(Function.identity(),
                artifact -> MavenInterface.locateArtifact(artifact.toAetherArtifact(version),
                        artifact.enterprise ? new String[] {"https://repository.hazelcast.com/release"} : new String[] {})
                        .toFile()));
    }
}
