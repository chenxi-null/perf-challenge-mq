package io.openmessaging.util;

/**
 * @author chenxi
 * @date 2021/10/31
 */
public class ByteUtil {

    public static long bytesToLong(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }
}
