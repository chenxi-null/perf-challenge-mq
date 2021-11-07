package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.MessageQueue;
import io.openmessaging.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/11
 */
public abstract class BaseTest {

    private static final Logger log = LoggerFactory.getLogger(BaseTest.class);

    static Config config = Config.getInstance();

    private DefaultMessageQueueImpl mq;

    protected DefaultMessageQueueImpl getMQ() {
        return this.mq;
    }

    protected void setMQ(DefaultMessageQueueImpl mq) {
        this.mq = mq;
    }

    @BeforeEach
    void beforeEach() throws IOException {
        int size = "content-1-10001_1".getBytes().length;
        config.setOneWriteMaxDataSize(100);
        config.setBatchWriteMemBufferSizeThreshold(size + size);

        String testRootDir = "./output/essd/mqx";
        Path testRootDirPath = Paths.get(testRootDir);
        config.setRootDir(testRootDir);
        System.out.println("reset rootDir: " + testRootDirPath.toFile().getAbsolutePath());

        if (Files.exists(testRootDirPath)) {
            assertTrue(FileUtil.safeDeleteDirectory(new File(testRootDir)));
            System.out.println("deleted ssd dir");
        }

        config.setPmemMsgHeapSize(8388608);
        config.setPmemIndexHeapSize(8388608);
        config.setPmemIndexMemoryBlockSize(1024);
        String pmemDir = new File("./output/pmem/mqx").getCanonicalPath();
        config.setPmemDir(pmemDir);
        if (Files.exists(Paths.get(pmemDir))) {
            assertTrue(FileUtil.safeDeleteDirectory(pmemDir));
            System.out.println("deleted pmem dir: " + pmemDir);
        }

        System.out.println("---- finish file cleanup ---");

        config.setEnablePmem(false);
        this.mq = new DefaultMessageQueueImpl();
        System.out.println("created mq instance");
    }

    @AfterEach
    void tearDown() {
        System.out.println("--- cleanup: mq stop ---");
        mq.stop();
    }

    /*
    topic1: 10001(1, 2, 3), 10002(1), 10003(1)
    topic2: 10001(1, 2), 10002(1)
    //diff msg size
    topic3: 12345(1)
    topic4: 23456(1)
     */
    int writeTestData(MessageQueue mq) throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(6);

        prepareWriteTestData(startLatch, finishLatch, mq, "topic1", 10001, Arrays.asList(
                "content-1-10001_1",
                "content-1-10001_2",
                "content-1-10001_3"));

        prepareWriteTestData(startLatch, finishLatch, mq, "topic1", 10002, "content-1-10002_1");
        prepareWriteTestData(startLatch, finishLatch, mq, "topic1", 10003, "content-1-10003_1");

        prepareWriteTestData(startLatch, finishLatch, mq, "topic2", 10001, Arrays.asList(
                "content-2-10001_1",
                "content-2-10001_2"));

        prepareWriteTestData(startLatch, finishLatch, mq, "topic3", 12345, "content-3-12345_*****");

        prepareWriteTestData(startLatch, finishLatch, mq, "topic4", 23456, "content-4-23456");

        startLatch.countDown();
        finishLatch.await();
        System.out.println(">>> finish write test data, size: 9");
        return 9;
    }

    void prepareWriteTestData(CountDownLatch latch, CountDownLatch finishLatch,
                              MessageQueue mq, String topic, int queueId, List<String> dataList) {
        new Thread(() -> {
            try {
                latch.await();
                for (String data : dataList) {
                    log.info("mq append, ({}, {}), {}", topic, queueId, data);
                    mq.append(topic, queueId, toByteBuffer(data));
                }
                finishLatch.countDown();
            } catch (Throwable e) {
                log.error("thread ex", e);
            }
        }).start();
    }

    void prepareWriteTestData(CountDownLatch latch, CountDownLatch finishLatch,
                              MessageQueue mq, String topic, int queueId, String data) {
        prepareWriteTestData(latch, finishLatch, mq, topic, queueId, Collections.singletonList(data));
    }

    // topic1: 10001(1, 2, 3, 4), 10002, 10003
    // topic2: 10001(1, 2), 10002(1)
    int writeTestData2(MessageQueue mq) {
        mq.append("topic1", 10001, toByteBuffer("content-1-10001_4"));
        mq.append("topic2", 10002, toByteBuffer("content-1-10002_1"));
        return 2;
    }

    private final ThreadLocal<ByteBuffer> wroteByteBufferContext =
            ThreadLocal.withInitial(() -> ByteBuffer.allocate(config.getOneWriteMaxDataSize()));

    ByteBuffer toByteBuffer(String s) {
        ByteBuffer wroteByteBuffer = wroteByteBufferContext.get();
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
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
