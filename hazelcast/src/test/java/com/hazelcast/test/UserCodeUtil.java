package com.hazelcast.test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Utility methods to facilitate referencing files from {@code src/test/class} which are not part of the classpath at
 * test execution time.
 */
public class UserCodeUtil {

    public static final String CLASS_DIRECTORY = "src/test/class";
    public static final File CLASS_DIRECTORY_FILE = new File(CLASS_DIRECTORY);

    private UserCodeUtil() {
    }

    /** @return a File for the given path, relative to src/test/class */
    public static File fileRelativeToBinariesFolder(String path) {
        return new File(CLASS_DIRECTORY_FILE, path);
    }

    /**
     * TODO What's the application of this?
     * @return a File for the given path, relative to src/test/class
     */
    public static String pathRelativeToBinariesFolder(String path) {
        return fileRelativeToBinariesFolder(path).toString();
    }

    public static URL urlFromFile(File f) {
        try {
            return f.toURI().toURL();
        } catch (MalformedURLException e) {
            throw sneakyThrow(e);
        }
    }
}
