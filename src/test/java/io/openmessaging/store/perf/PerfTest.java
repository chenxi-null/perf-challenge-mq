package io.openmessaging.store.perf;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.MessageQueue;
import io.openmessaging.util.FileUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.util.Util.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author chenxi20
 * @date 2021/11/6
 */
@Getter
public class PerfTest {

    private static final Logger log = LoggerFactory.getLogger(PerfTest.class);

    static Config config = Config.getInstance();

    private DefaultMessageQueueImpl mq;

    @BeforeEach
    void beforeEach() throws IOException {
        String testRootDir = "./output/essd/mqx";
        Path testRootDirPath = Paths.get(testRootDir);
        config.setRootDir(testRootDir);
        System.out.println("reset rootDir: " + testRootDirPath.toFile().getAbsolutePath());

        if (Files.exists(testRootDirPath)) {
            assertTrue(FileUtil.safeDeleteDirectory(new File(testRootDir)));
            System.out.println("deleted ssd dir");
        }
        System.out.println("---- finish file cleanup ---");

        config.setEnablePmem(false);
        this.mq = new DefaultMessageQueueImpl();
        System.out.println("created mq instance");
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void test() throws InterruptedException {
        writePhase();

        writeAndReadPhase();
    }

    //public static long MAX_SPACE_SIZE = 120L * 1024 * 1024 * 1024;
    public static long MAX_SPACE_SIZE = 100 * 1024 * 1024;

    public static final int AWAIT_TIMEOUT_SECONDS = 20;

    public static int THREAD_NUM = 50;

    public static int TOPIC_NUM = 100;

    public static int QUEUE_NUM = 5000;

    private final Map<Integer, List<Integer>> threadIdxToTopicIndexes = new HashMap<>();

    private final Map<Integer, List<Integer>> topicIdxToQueueIds = new HashMap<>();

    private final CountDownLatch latch = new CountDownLatch(THREAD_NUM);

    private final AtomicInteger errorCounter = new AtomicInteger();

    private final AtomicLong wroteSize = new AtomicLong();

    private final AtomicInteger wroteTime = new AtomicInteger();

    private void writePhase() throws InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);

        allocateQueueForTopics(topicIdxToQueueIds);
        allocateTopicForThreads(threadIdxToTopicIndexes);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_NUM; i++) {
            threadPool.submit(new WriteTask(i, ByteBuffer.allocate(17_000),
                    threadIdxToTopicIndexes, topicIdxToQueueIds,
                    mq, MAX_SPACE_SIZE, wroteSize, errorCounter, this));
        }
        boolean stop = latch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        long costTime = System.currentTimeMillis() - startTime;

        log.info("costTime: " + costTime);
        log.info("wrote size: " + wroteSize.get());
        log.info("wrote time: " + wroteTime.get());
        assertEquals(0, errorCounter.get());
        assertTrue(stop);
    }

    private void allocateQueueForTopics(Map<Integer, List<Integer>> topicIdxToQueueIds) {
        for (int topicIdx = 0; topicIdx < TOPIC_NUM; topicIdx++) {
            ArrayList<Integer> queueIds = new ArrayList<>();
            topicIdxToQueueIds.put(topicIdx, queueIds);
            int queueNum = ThreadLocalRandom.current().nextInt(1, QUEUE_NUM);
            for (int i = 0; i < queueNum; i++) {
                queueIds.add(10000 + i);
            }
        }
    }

    private void allocateTopicForThreads(Map<Integer, List<Integer>> threadIdxToTopicIndexes) {
        // TODO: random
        for (int i = 0; i < THREAD_NUM; i++) {
            threadIdxToTopicIndexes.put(i, new ArrayList<>(2));
            threadIdxToTopicIndexes.get(i).add(i);
            threadIdxToTopicIndexes.get(i).add(i + THREAD_NUM);
        }
    }

    @RequiredArgsConstructor
    static class WriteTask implements Runnable {

        private final int threadIdx;

        private final ByteBuffer byteBuffer;

        private final Map<Integer, List<Integer>> threadIdxToTopicIndexes;

        private final Map<Integer, List<Integer>> topicIdxToQueueIds;

        private final MessageQueue mq;

        private final long maxSpaceSize;

        private final AtomicLong wroteSize;

        private volatile boolean stop = false;

        private final AtomicInteger errorCounter;

        private final PerfTest perfTest;

        @Override
        public void run() {
            if (stop) {
                try {
                    Thread.sleep(10000);
                    return;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                List<Integer> topicIndexes = threadIdxToTopicIndexes.get(threadIdx);

                for (int round = 0; round < Integer.MAX_VALUE; round++) {
                    if (stop) {
                        break;
                    }
                    for (Integer topicIndex : topicIndexes) {
                        if (stop) {
                            break;
                        }
                        List<Integer> queueIds = topicIdxToQueueIds.get(topicIndex);
                        for (Integer queueId : queueIds) {
                            long currSpaceSize = wroteSize.get();
                            if (currSpaceSize >= maxSpaceSize) {
                                log.info(">>> stop because space is full, size: " + currSpaceSize);
                                stop = true;
                                break;
                            }
                            ByteBuffer data = genData(byteBuffer, topicIndex, queueId, round);
                            wroteSize.addAndGet(data.remaining());
                            perfTest.wroteTime.incrementAndGet();
                            log.debug("wrote size : " + wroteSize.get());
                            mq.append("topic" + topicIndex, queueId, data);
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("thread occur ex", e);
                errorCounter.incrementAndGet();
            } finally {
                perfTest.latch.countDown();
            }
        }
    }


    // ---------------------- utils ----------------------

    static ByteBuffer genData(ByteBuffer b, Integer topicIndex, int queueId, int round) {
        // [800B, 1700B]
        int size = ThreadLocalRandom.current().nextInt(100, 17000);
        String prefix = "topic" + topicIndex + "-" + queueId + "-" + round + "-" + System.currentTimeMillis() + "-";
        byte[] bytes = prefix.getBytes();

        b.rewind();
        b.put(bytes);
        for (int i = bytes.length; i < size; i++) {
            b.put((byte) (i % 256));
        }

        // prepare for byteBuffer.get
        b.clear();
        return b;
    }

    //----------------------------------------------------

    private void writeAndReadPhase() {
        // keep writing
        // pick two kinds of queue, read data from those queues util all data is read
        //  1. read from current max offset
        //  2. read from min offset
    }
}

/*
评测程序会创建10~50个线程，每个线程随机若干个topic（topic总数<=100），每个topic有N个queueId（1 <= N <= 5,000），持续调用append接口进行写入；
评测保证线程之间数据量大小相近（topic之间不保证），每个data的大小为100B-17KiB区间随机（伪随机数程度的随机），数据几乎不可压缩，需要写入总共75GiB的数据量。

保持刚才的写入压力，随机挑选50%的队列从当前最大点位开始读取，剩下的队列均从最小点位开始读取（即从头开始），再写入总共50GiB后停止全部写入，读取持续到没有数据，然后停止。
 */
