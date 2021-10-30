package io.openmessaging.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author chenxi20
 * @date 2021/10/9
 */
public class FileUtil {

    public static void createFileIfNotExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

    public static void createDirIfNotExists(String strPath) throws IOException {
        createDirIfNotExists(Paths.get(strPath));
    }

    public static void createDirIfNotExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
    }

    public static boolean safeDeleteFile(File file) {
        if (file.getAbsolutePath().contains("mq-sample")) {
            return file.delete();
        }
        return false;
    }

    public static boolean safeDeleteDirectory(String strDir) {
        return safeDeleteDirectory(new File(strDir));
    }

    public static boolean safeDeleteDirectory(File dir) {
        if (dir.getAbsolutePath().contains("mq-sample")) {
            return deleteDirectory(dir);
        }
        return false;
    }

    private static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
