package io.openmessaging.util;

/**
 * @author chenxi20
 * @date 2021/10/10
 */
public class Util {

    public static void assertTrue(boolean expr) {
        if (!expr) {
            throw new AssertionError();
        }
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
    }
}
