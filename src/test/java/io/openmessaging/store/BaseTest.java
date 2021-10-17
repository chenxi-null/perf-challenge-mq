package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.MessageQueue;
import io.openmessaging.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/11
 */
public abstract class BaseTest {

    public static int maxMsgSize = 100;

    @BeforeAll
    static void beforeAll() {
        int size = "content-1-10001_1".getBytes().length;
        Config.getInstance().setOneWriteMaxDataSize(maxMsgSize);
        Config.getInstance().setBatchWriteMemBufferSizeThreshold(size + size);

        String testRootDir = "/Users/chenxi20/Downloads/code/chenxi-projects/mq-sample/output/essd/mqx";
        Path testRootDirPath = Paths.get(testRootDir);
        Config.getInstance().setRootDir(testRootDir);
        System.out.println("reset rootDir");

        if (Files.exists(testRootDirPath)) {
            assertTrue(FileUtil.safeDeleteDirectory(new File(testRootDir)));
            System.out.println("deleted dir");
        }
        System.out.println("---- finish file cleanup ---");
    }

    private DefaultMessageQueueImpl mq;

    void setMQ(DefaultMessageQueueImpl mq) {
        this.mq = mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null) {
            System.out.println("--- cleanup: mq stop ---");
            mq.stop();
        }
    }

    /*
    topic1: 10001(1, 2, 3), 10002(1), 10003(1)
    topic2: 10001(1), 10002(1)
    //diff msg size
    topic3: 12345(1)
    topic4: 23456(1)
     */
    int writeTestData(MessageQueue mq) {
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_1"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_1"));

        mq.append("topic1", 10002, toByteBuffer("content-1-10002_1"));
        mq.append("topic1", 10003, toByteBuffer("content-1-10003_1"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_2"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_3"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_2"));

        mq.append("topic3", 12345, toByteBuffer("content-3-12345_*****"));

        mq.append("topic4", 23456, toByteBuffer("content-4-23456"));
        return 7 + 2;
    }

    // topic1: 10001(1, 2, 3, 4), 10002, 10003
    // topic2: 10001(1, 2), 10002
    int writeTestData2(MessageQueue mq) {
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_4"));
        mq.append("topic2", 10002, toByteBuffer("content-1-10002_1"));
        return 2;
    }

    private final ByteBuffer wroteByteBuffer = ByteBuffer.allocate(maxMsgSize);

    ByteBuffer toByteBuffer(String s) {
        wroteByteBuffer.clear();
        wroteByteBuffer.put(s.getBytes(StandardCharsets.ISO_8859_1));
        // invoke flip before write
        wroteByteBuffer.flip();
        return wroteByteBuffer;
    }

    String toString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.limit()];
        buffer.flip();
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
