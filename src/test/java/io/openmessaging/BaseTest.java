package io.openmessaging;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/8
 */
class BaseTest {

    MessageQueue mq;

    @BeforeAll
    static void beforeAll() {
        String testRootDir = "/Users/chenxi20/Downloads/code/chenxi-projects/mq-sample/output";
        String commitLogFile = testRootDir + "/essd" + "/commitLog";
        Config.getInstance().setCommitLogFile(commitLogFile);
        String consumerQueueRootDir = testRootDir + "/essd";
        Config.getInstance().setConsumerQueueRootDir(consumerQueueRootDir);
        System.out.println("reset commitLogFile: " + commitLogFile);
        System.out.println("reset consumerQueueRootDir: " + consumerQueueRootDir);
    }

    @Test
    void baseTest() {
        doBaseTest(new DefaultMessageQueueImpl());
    }

    @Test
    void baseTest_InMemoryImpl() {
        doBaseTest(new InMemoryImpl());
    }

    void doBaseTest(MessageQueue mq) {

        mq.append("topic1", 10001, toByteBuffer("content-1-10001_1"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_1"));

        mq.append("topic1", 10002, toByteBuffer("content-1-10002_1"));
        mq.append("topic1", 10003, toByteBuffer("content-1-10003_1"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_2"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_3"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_2"));

        // topic1: 10001(1, 2, 3), 10002, 10003
        // topic2: 10001, 10002
        assertAll(
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("wrong-topic", 10001, 0, 1);
                    assertTrue(map.isEmpty());
                },
                () -> {
                    Map<Integer, ByteBuffer> map = mq.getRange("topic1", 10002, 0, 10);
                    assertEquals(1, map.size());
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
                }
        );
    }

    private ByteBuffer toByteBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    private String toString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.capacity()];
        buffer.rewind();
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
