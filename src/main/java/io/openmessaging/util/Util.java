package io.openmessaging.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenxi
 * @date 2021/10/10
 */
public class Util {

    private static final Logger log = LoggerFactory.getLogger(Util.class);

    public static String buildKey(String topic, int queueId) {
        return topic + "-" + queueId;
    }

    public static void assertTrue(boolean expr) {
        assertTrue(expr, "");
    }

    public static void assertTrue(boolean expr, String desc) {
        if (!expr) {
            throw new AssertionError(desc);
        }
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Thread.interrupted();
            log.error("occur InterruptedException", e);
        }
    }

    public static void assertNotNull(Object o) {
        if (o == null) {
            throw new NullPointerException("can't be null");
        }
    }
}
