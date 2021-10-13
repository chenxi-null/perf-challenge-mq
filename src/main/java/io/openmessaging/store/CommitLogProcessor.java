package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author chenxi20
 * @date 2021/10/13
 */
public class CommitLogProcessor {

    //----------------------------------------------------

    /*
    enhancement:
    - file partition
    - MappedByteBuffer
    - batch write
     */

    //----------------------------------------------------

    // caller:
    //      put data and wait by Future
    //
    // coordinator:
    //      - receive data
    //      - flush disk when satisfied
    //      - notify caller
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
    //      if (currentTime - startTime > timeThreshold), then flush
    //
    // (3)
    // if (itemSize > threadSizeThreshold)
    //
    // notify mechanism: Future


    private static final Logger log = LoggerFactory.getLogger(CommitLogProcessor.class);

    private final BlockingQueue<Item> blockingQueue = new LinkedBlockingQueue<>();

    private final Store store;

    public CommitLogProcessor(Store store) {
        this.store = store;
        init();
    }

    private void init() {
        ReadyBuffer readyBuffer = new ReadyBuffer(store.getCommitLog());

        BatchWriteTask batchWriteTask = new BatchWriteTask(blockingQueue, readyBuffer);
        new Thread(batchWriteTask).start();

        TimeWindowCheckTask timeWindowCheckTask = new TimeWindowCheckTask(readyBuffer);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                timeWindowCheckTask, 500, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * @return queueOffset
     */
    public long write(String topic, int queueId, ByteBuffer data) throws InterruptedException {
        Item item = new Item(topic, queueId, data);
        blockingQueue.put(item);

        Long queueOffset;
        queueOffset = item.getDoneFuture().get();
        Util.assertNotNull(queueOffset);
        return queueOffset;
    }

    private static class ReadyBuffer {

        private final CommitLog commitLog;

        private int bufferSize = 0;

        private long timeWindowStartTime = System.currentTimeMillis();

        private final List<Item> items = new ArrayList<>();

        private ReadyBuffer(CommitLog commitLog) {
            this.commitLog = commitLog;
        }

        public int size() {
            return this.bufferSize;
        }

        public void append(Item item) {
            items.add(item);
            this.bufferSize += item.getData().capacity();
        }

        // lock
        public void write() throws IOException {
            if (size() > 0) {
                this.commitLog.writeAndNotify(items);
            }

            // clear up
            updateTimeWindowStartTime();
            items.clear();
            bufferSize = 0;
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
    static class BatchWriteTask implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(BatchWriteTask.class);

        private final BlockingQueue<Item> blockingQueue;

        private final ReadyBuffer readyBuffer;

        BatchWriteTask(BlockingQueue<Item> blockingQueue, ReadyBuffer readyBuffer) {
            this.blockingQueue = blockingQueue;
            this.readyBuffer = readyBuffer;
        }

        @Override
        public void run() {
            //noinspection InfiniteLoopStatement
            while (true) {
                Item item;
                try {
                    item = blockingQueue.take();
                } catch (InterruptedException e) {
                    log.error("InterruptedException", e);
                    continue;
                }
                readyBuffer.append(item);
                if (readyBuffer.size() >= Config.getInstance().getBatchWriteMemBufferSizeThreshold()) {
                    try {
                        readyBuffer.write();
                    } catch (Exception e) {
                        log.error("failed to write", e);
                    }
                }
            }
        }
    }

    // check the wait time of readyBuffer
    // batch write if timeout
    static class TimeWindowCheckTask implements Runnable {

        private final ReadyBuffer readyBuffer;

        TimeWindowCheckTask(ReadyBuffer readyBuffer) {
            this.readyBuffer = readyBuffer;
        }

        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            if (currentTime - readyBuffer.getTimeWindowStartTime()
                    >= Config.getInstance().getBatchWriteWaitTimeThreshold()) {
                try {
                    readyBuffer.write();
                } catch (Exception e) {
                    log.error("failed to write", e);
                }
            }
        }
    }
}
