package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.common.NamedThreadFactory;
import io.openmessaging.common.StopWare;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chenxi
 * @date 2021/10/13
 */
public class CommitLogProcessor implements StopWare, MsgStoreProcessor {

    // ----------------------------------------------------

    /*
     * enhancement:
     * - file partition
     * - MappedByteBuffer
     * - batch write
     */

    // ----------------------------------------------------

    // caller:
    // put data and wait by Future
    //
    // coordinator:
    // - receive data
    // - flush disk when satisfied
    // - notify caller
    //
    // [. . queue . . ] ---> [ .. readyBuffer .. ]
    //
    //
    // check the 'readyBuffer', the flush condition:
    //
    // (1)
    // if (memBuffer > dataSizeThreshold), then flush
    //
    // (2)
    // start a scheduled task that check in a fixed rate
    // if (currentTime - startTime > timeThreshold), then flush
    //
    // (3)
    // if (itemSize > threadSizeThreshold)
    //
    // notify mechanism: Future

    private static final Logger log = LoggerFactory.getLogger(CommitLogProcessor.class);

    private final BlockingQueue<Item> blockingQueue = new LinkedBlockingQueue<>();

    private final Store store;

    private final ScheduledExecutorService timeWindowCheckScheduledService = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("batchWriteTimeCheckTask"));

    private final ExecutorService batchWriteTaskService = Executors
            .newSingleThreadExecutor(new NamedThreadFactory("batchWriteTask"));

    public CommitLogProcessor(Store store) {
        this.store = store;
        init();
    }

    private void init() {
        ReadyBuffer readyBuffer = new ReadyBuffer(store.getCommitLog());

        BatchWriteTask batchWriteTask = new BatchWriteTask(blockingQueue, readyBuffer);
        batchWriteTaskService.submit(batchWriteTask);

        TimeWindowCheckTask timeWindowCheckTask = new TimeWindowCheckTask(readyBuffer);
        timeWindowCheckScheduledService.scheduleAtFixedRate(
                timeWindowCheckTask, 500,
                Config.getInstance().getBatchWriteTimeWindowCheckInternal(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        timeWindowCheckScheduledService.shutdownNow();
        batchWriteTaskService.shutdownNow();
        try {
            if (!timeWindowCheckScheduledService.awaitTermination(1, TimeUnit.SECONDS)) {
                log.warn("failed to stop timeWindowCheckScheduledService");
            }
            if (!batchWriteTaskService.awaitTermination(1, TimeUnit.SECONDS)) {
                log.warn("failed to stop batchWriteTaskService");
            }
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
        log.info("stopped background tasks");
    }

    /**
     * @return queueOffset
     */
    @Override
    public long write(String topic, int queueId, ByteBuffer data) throws InterruptedException {
        log.trace("write ({}, {})", topic, queueId);
        Item item = new Item(topic, queueId, data);
        blockingQueue.put(item);

        Long queueOffset;
        queueOffset = item.getDoneFuture().get();
        Util.assertNotNull(queueOffset);
        return queueOffset;
    }

    @Override
    public ByteBuffer getData(String topic, int queueId, long offset) throws Exception {
        long commitLogOffset = store.getTopicQueueTable().getPhyOffset(topic, queueId, offset);
        if (commitLogOffset < 0) {
            return null;
        }
        return store.getCommitLog().getData(commitLogOffset);
    }

    private static class ReadyBuffer {

        private final CommitLog commitLog;

        private volatile int bufferSize = 0;

        private volatile long timeWindowStartTime = System.currentTimeMillis();

        private final List<Item> items = new ArrayList<>();

        private final ReentrantLock lock = new ReentrantLock();

        private ReadyBuffer(CommitLog commitLog) {
            this.commitLog = commitLog;
        }

        public int size() {
            return this.bufferSize;
        }

        // only one thread invoke
        public void append(Item item) {
            lock.lock();
            try {
                items.add(item);
                // noinspection NonAtomicOperationOnVolatileField
                this.bufferSize += item.getData().limit(); // data.remaining();
            } finally {
                lock.unlock();
            }
        }

        public void write() throws IOException {
            lock.lock();
            try {
                updateTimeWindowStartTime();

                if (size() <= 0) {
                    log.trace("size = 0");
                    return;
                }

                this.commitLog.writeAndNotify(items);

                // clear up
                items.clear();
                bufferSize = 0;
            } finally {
                lock.unlock();
            }
        }

        public void updateTimeWindowStartTime() {
            timeWindowStartTime = System.currentTimeMillis();
        }

        public long getTimeWindowStartTime() {
            return timeWindowStartTime;
        }
    }

    // fetch data from write-queue and put into readyBuffer
    // batch write
    static class BatchWriteTask implements Runnable, StopWare {

        private static final Logger log = LoggerFactory.getLogger(BatchWriteTask.class);

        private final BlockingQueue<Item> blockingQueue;

        private final ReadyBuffer readyBuffer;

        private volatile boolean running = true;

        BatchWriteTask(BlockingQueue<Item> blockingQueue, ReadyBuffer readyBuffer) {
            this.blockingQueue = blockingQueue;
            this.readyBuffer = readyBuffer;
        }

        @Override
        public void run() {
            while (running) {
                Item item;
                try {
                    item = blockingQueue.take();
                } catch (InterruptedException e) {
                    log.info("receive interrupt signal, stop current task");
                    break;
                }
                readyBuffer.append(item);
                if (readyBuffer.size() >= Config.getInstance().getBatchWriteMemBufferSizeThreshold()) {
                    try {
                        readyBuffer.write();
                    } catch (Throwable e) {
                        log.error("failed to write", e);
                    }
                }
            }
        }

        @Override
        public void stop() {
            this.running = false;
        }
    }

    // check the wait time of readyBuffer
    // batch write if timeout
    static class TimeWindowCheckTask implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

        private final ReadyBuffer readyBuffer;

        TimeWindowCheckTask(ReadyBuffer readyBuffer) {
            this.readyBuffer = readyBuffer;
        }

        @Override
        public void run() {
            try {
                long currentTime = System.currentTimeMillis();
                long elapsedTime = currentTime - readyBuffer.getTimeWindowStartTime();
                if (elapsedTime >= Config.getInstance().getBatchWriteWaitTimeThreshold()
                        && readyBuffer.size() > 0) {
                    try {
                        readyBuffer.write();
                    } catch (Throwable e) {
                        log.error("failed to write", e);
                    }
                }
            } catch (Throwable e) {
                log.error("failed to --->", e);
            }
        }
    }
}
