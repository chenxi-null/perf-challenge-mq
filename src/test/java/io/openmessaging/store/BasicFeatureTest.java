package io.openmessaging.store;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.InMemoryImpl;
import io.openmessaging.MessageQueue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/8
 */
class BasicFeatureTest extends BaseTest {

    @Test
    void baseTest() {
        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        setMQ(mq);
        doBaseTest(mq);
    }

    @Test
    void baseTest_InMemoryImpl() {
        doBaseTest(new InMemoryImpl());
    }


    void doBaseTest(MessageQueue mq) {

        writeTestData(mq);

        // topic1: 10001(1, 2, 3), 10002, 10003
        // topic2: 10001, 10002
        assertAll(
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("wrong-topic", 10001, 0, 1);
                    assertTrue(map.isEmpty(), "wrong-topic");
                },
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("topic1", 10002, 0, 10);
                    assertEquals(1, map.size(), "fetch all msg, (t1, q2)");
                    assertEquals("content-1-10002_1", toString(map.get(0)));
                },
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("topic1", 10001, 2, 10);
                    assertEquals(1, map.size());
                    assertEquals("content-1-10001_3", toString(map.get(0)));
                },
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("topic1", 10001, 1, 2);
                    assertEquals(2, map.size());
                    assertEquals("content-1-10001_2", toString(map.get(0)));
                    assertEquals("content-1-10001_3", toString(map.get(1)));
                },
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("topic2", 10001, 0, 10);
                    assertEquals(2, map.size());
                    assertEquals("content-2-10001_1", toString(map.get(0)));
                    assertEquals("content-2-10001_2", toString(map.get(1)));
                },
                () -> {
                }
        );
    }
}
