package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.MessageQueue;
import io.openmessaging.util.FileUtil;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/11
 */
public abstract class BaseTest {

    @BeforeAll
    static void beforeAll() {
        String testRootDir = "/Users/chenxi20/Downloads/code/chenxi-projects/mq-sample/output/essd";
        String commitLogFile = testRootDir + "/commitLog";

        Config.getInstance().setCommitLogFile(commitLogFile);
        Config.getInstance().setConsumerQueueRootDir(testRootDir);
        System.out.println("reset commitLogFile: " + commitLogFile);
        System.out.println("reset consumerQueueRootDir: " + testRootDir);

        for (File file : Objects.requireNonNull(new File(testRootDir).listFiles())) {
            if (file.isFile()) {
                assertTrue(file.delete());
            } else {
                FileUtil.deleteDirectory(file);
            }
        }
        System.out.println("deleted files");
    }

    // topic1: 10001(1, 2, 3), 10002, 10003
    // topic2: 10001(1, 2)
    int writeTestData(MessageQueue mq) {
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_1"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_1"));

        mq.append("topic1", 10002, toByteBuffer("content-1-10002_1"));
        mq.append("topic1", 10003, toByteBuffer("content-1-10003_1"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_2"));
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_3"));

        mq.append("topic2", 10001, toByteBuffer("content-2-10001_2"));
        return 7;
    }

    // topic1: 10001(1, 2, 3, 4), 10002, 10003
    // topic2: 10001(1, 2), 10002
    int writeTestData2(MessageQueue mq) {
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_4"));
        mq.append("topic2", 10002, toByteBuffer("content-1-10002_1"));
        return 2;
    }

    ByteBuffer toByteBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    String toString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.capacity()];
        buffer.rewind();
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
