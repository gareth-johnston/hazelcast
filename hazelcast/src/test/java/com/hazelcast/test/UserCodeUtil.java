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

package com.hazelcast.test;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility methods to facilitate referencing files from {@code src/test/class} which are not part of the classpath at
 * test execution time.
 */
public class UserCodeUtil {
    public static final String CLASS_DIRECTORY = "src/test/class";
    public static final Path CLASS_DIRECTORY_FILE = Paths.get(CLASS_DIRECTORY);

    private UserCodeUtil() {
    }

    /** @return a {@link Path} for the given path, relative to src/test/class */
    public static Path fileRelativeToBinariesFolder(String path) {
        return CLASS_DIRECTORY_FILE.resolve(path);
    }

    public static URL urlFromFile(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            throw sneakyThrow(e);
        }
    }
}
