package com.hazelcast.test.starter;

import static com.hazelcast.internal.util.Preconditions.checkState;

import org.apache.commons.io.FilenameUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.internal.impl.DefaultLocalRepositoryProvider;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.LocalRepositoryManager;
import org.eclipse.aether.repository.NoLocalRepositoryManagerException;
import org.h2.util.StringUtils;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.internal.util.StringUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MavenInterface {
    private static final Path MAVEN_REPOSITORY;
    private static final LocalRepositoryManager REPOSITORY_MANAGER;

    static {
        try {
            final DefaultLocalRepositoryProvider repositoryProvider = new DefaultLocalRepositoryProvider();

            repositoryProvider
                    .setLocalRepositoryManagerFactories(Collections.singletonList(new SimpleLocalRepositoryManagerFactory()));

            // You can query this dynamically with the command:
            // mvn help:evaluate -Dexpression=settings.localRepository --quiet --batch-mode -DforceStdout
            //
            // But can be problematic to parse when additional VM arguments print logging information to stdout as well
            // https://github.com/hazelcast/hazelcast/issues/25451
            MAVEN_REPOSITORY = Paths.get(System.getProperty("user.home")).resolve(".m2").resolve("repository");

            if (Files.exists(MAVEN_REPOSITORY)) {
                final LocalRepository localRepo = new LocalRepository(MAVEN_REPOSITORY.toFile());
                REPOSITORY_MANAGER = repositoryProvider.newLocalRepositoryManager(MavenRepositorySystemUtils.newSession(),
                        localRepo);
            } else {
                throw new NoSuchFileException(MAVEN_REPOSITORY.toString(), null, "Maven repository");
            }
        } catch (final IOException | NoLocalRepositoryManagerException e) {
            throw new RuntimeException(e);
        }
    }

    /** @return a {@link Path} to the artifact in the local Maven repository, downloading if required */
    public static Path locateArtifact(Artifact artifact, String... remoteRepositories) {
        final Path localCopy = MAVEN_REPOSITORY
                .resolve(REPOSITORY_MANAGER.getPathForLocalArtifact(artifact) + FilenameUtils.EXTENSION_SEPARATOR
                        + (StringUtils.isNullOrEmpty(artifact.getExtension()) ? "jar" : artifact.getExtension()));

        if (!Files.exists(localCopy)) {
            downloadArtifact(artifact, remoteRepositories);

            if (!Files.exists(localCopy)) {
                throw new UncheckedIOException(new NoSuchFileException(localCopy.toString(), null,
                        MessageFormat.format("{0} not found after download", artifact)));
            }
        }

        return localCopy;
    }

    private static String getMvn() {
        return OsHelper.isWindows() ? "mvn.cmd" : "mvn";
    }

    private static void downloadArtifact(Artifact artifact, String... remoteRepositories) {
        // It's also possible to download this via Aether but I was unable to get this working
        final ProcessBuilder builder = new ProcessBuilder(
                buildMavenCommand(artifact, remoteRepositories).toArray(String[]::new)).inheritIO();
        try {
            final Process process = builder.start();
            final boolean successful = process.waitFor(2, TimeUnit.MINUTES);
            checkState(successful, "Maven dependency:get timed out");
            checkState(process.exitValue() == 0, "Maven dependency:get failed");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(MessageFormat.format("Problem in invoking Maven dependency:get {0}", artifact), e);
        }
    }

    private static Stream<String> buildMavenCommand(Artifact artifact, String... remoteRepositories) {
        final Stream.Builder<String> builder = Stream.builder();

        builder.add(getMvn());

        builder.add("dependency:get");
        builder.add("-DgroupId=" + artifact.getGroupId());
        builder.add("-DartifactId=" + artifact.getArtifactId());
        builder.add("-Dversion=" + artifact.getVersion());
        builder.add("--quiet");
        builder.add("--batch-mode");

        if (!StringUtil.isNullOrEmpty(artifact.getClassifier())) {
            builder.add("-Dclassifier=" + artifact.getClassifier());
        }

        if (remoteRepositories.length != 0) {
            builder.add("-DremoteRepositories=" + String.join(",", remoteRepositories));
        }

        return builder.build();
    }
}
