package io.openmessaging.util;

import java.io.File;

/**
 * @author chenxi20
 * @date 2021/10/9
 */
public class FileUtil {

    public static boolean safeDeleteFile(File file) {
        if (file.getAbsolutePath().contains("-contest-1")) {
            return file.delete();
        }
        return false;
    }

    //public static boolean safeDeleteDirectory(String filepath) {
    //    if (filepath.contains("-contest-1")) {
    //        return deleteDirectory(new File(filepath));
    //    }
    //    return false;
    //}

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
