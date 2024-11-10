package io.openmessaging.util;

import java.util.Objects;
import java.util.zip.CRC32;

/**
 * @author chenxi
 * @date 2021/10/22
 */
public class ChecksumUtil {

    public static int get(long queueOffset, long physicalOffset) {
        byte[] bytes = (queueOffset + physicalOffset + "").getBytes();
        return get(bytes, 0, bytes.length);
    }

    public static boolean check(long queueOffset, long physicalOffset, int checksum) {
        return Objects.equals(get(queueOffset, physicalOffset), checksum);
    }

    public static int get(byte[] bytes, int offset, int length) {
        return crc32(bytes, offset, length);
    }

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }
}
