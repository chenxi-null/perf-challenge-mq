package io.openmessaging.util;

/**
 * @author chenxi20
 * @date 2021/10/22
 */
public class ChecksumUtil {

    public static int get(byte[] bytes, int offset, int length) {
        return 0;
    }

    public static int get(long queueOffset, long physicalOffset) {
        return 0;
    }

    public static boolean check(long queueOffset, long physicalOffset, int checksum) {
        return true;
    }
}
