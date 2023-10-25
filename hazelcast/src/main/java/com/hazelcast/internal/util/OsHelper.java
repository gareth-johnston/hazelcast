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

package com.hazelcast.internal.util;

/**
 * Helper methods related to operating system on which the code is actually running.
 */
public final class OsHelper {

    /**
     * OS name in lower case.
     */
    public static final String OS = System.getProperty("os.name").toLowerCase();

    private static final boolean IS_WINDOWS = OS.contains("windows");
    private static final boolean IS_MAC = (OS.contains("mac") || OS.contains("darwin"));
    private static final boolean IS_LINUX = OS.contains("nux");
    private static final boolean IS_UNIX_FAMILY = (OS.contains("nix") || OS.contains("nux") || OS.contains("aix"));

    private OsHelper() {
    }

    /**
     * Returns {@code true} if the system is Linux.
     *
     * @return {@code true} if the current system is Linux.
     */
    public static boolean isLinux() {
        return IS_LINUX;
    }

    /**
     * Returns {@code true} if the system is from Unix family.
     *
     * @return {@code true} if the current system is Unix/Linux/AIX.
     */
    public static boolean isUnixFamily() {
        return IS_UNIX_FAMILY;
    }

    /**
     * Returns {@code true} if the system is a Mac OS.
     *
     * @return {@code true} if the current system is Mac.
     */
    public static boolean isMac() {
        return IS_MAC;
    }

    /**
     * Returns {@code true} if the system is a Windows.
     *
     * @return {@code true} if the current system is a Windows one.
     */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    /**
     * Returns a file path string that replaces Windows `\\` file
     * separators with the Unix equivalent `/` if the current machine
     * is using Windows as its Operating System.
     *
     * @param path the file path string to convert
     * @return the file path string, with file separators set to `/`
     */
    public static String ensureUnixSeparators(final String path) {
        if (IS_WINDOWS) {
            return path.replace('\\', '/');
        }
        return path;
    }
}
