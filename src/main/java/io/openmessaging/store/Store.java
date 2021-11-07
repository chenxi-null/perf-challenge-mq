package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.common.NamedThreadFactory;
import io.openmessaging.common.StopWare;
import io.openmessaging.store.pmem.IndexHeap;
import io.openmessaging.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Persist:
//  - sync write commitLog
//      write disk
//      update mem topicQueueTable
//      update wrotePosition
//  - async build consumeQueue
//      read from commitLog, range is [consumeQueue.processedPhyOffset, commitLog.wrotePosition]
//      write disk
//      update processedPhyOffset
//
// Start and Recover:
//  - load files
//  - recover all consumeQueue files and get maxPhyOffsetOfConsumeQueue
//      check data item
//      load mem topicQueueTable
//  - recover commitLog from maxPhyOffsetOfConsumeQueue
//      check data item by crc
//      update wrotePosition
//      update mem topicQueueTable
//
// ----
//
// write:
//    - select storage-space
//    - write pmem & write ssd
//    - update memTable
//
// read:
//    - query memTable, route to corresponding storage-space
//    - read from pmem & ssd
//
// data recovery:
//    - recover ssd: consumeQueue, commitLog
//    - recover pmem: don't need to recover if using llpl tx, just need load memTable
//
// convention:
//  msg data in a queue is ordered by [ssd, pmem]
//
// pmem data storage structure:
//    msg: one msg one memory block
//
//    index(cq): handle position of memory block (index of msg)
//
//
public class Store implements StopWare {

    private static final Logger log = LoggerFactory.getLogger(Store.class);

    private final Config config = Config.getInstance();

    private CommitLog commitLog;

    private ConsumeQueue consumeQueue;

    private ConsumeQueueService consumeQueueService;

    private CommitLogProcessor commitLogProcessor;

    private ScheduledExecutorService consumeQueueSyncScheduledService =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("consumeQueueSyncTask"));

    private PmemMsgStoreProcessor pmemMsgStoreProcessor;

    private IndexHeap indexHeap;

    private StorageSelector storageSelector;

    public Store() throws IOException {
        start();
    }

    private void start() throws IOException {
        FileUtil.createDirIfNotExists(Config.getInstance().getRootDirPath());

        this.commitLog = new CommitLog(this);
        this.consumeQueue = new ConsumeQueue(this);
        this.consumeQueueService = new ConsumeQueueService(this);

        this.commitLogProcessor = new CommitLogProcessor(this);

        if (config.isEnablePmem()) {
            this.pmemMsgStoreProcessor = new PmemMsgStoreProcessor(this);
            this.pmemMsgStoreProcessor.start();

            this.indexHeap = new IndexHeap(this);
            this.indexHeap.start();
        }

        this.storageSelector = new StorageSelector(this);

        doStart();
    }

    // Start and Recover:
    //  - recover all consumeQueue files and get maxPhyOffsetOfConsumeQueue
    //      check data item
    //      load mem topicQueueTable
    //
    //  - recover commitLog from maxPhyOffsetOfConsumeQueue
    //      check data item by crc
    //      update wrote position
    //      update mem topicQueueTable
    //
    //  - sync logic data
    //
    //  |---------------|------------------|
    //              cq, table             log
    //
    //  |---------------|------------------|
    //                 cq              log, table
    //
    //  |---------------|------------------|
    //                               cq, log, table
    //
    public void doStart() throws IOException {
        dataRecovery();

        consumeQueue.syncFromCommitLog();

        // asyc write into consumeQueue
        if (Config.getInstance().isEnableConsumeQueueDataSync()) {
            consumeQueueSyncScheduledService
                    .scheduleAtFixedRate(consumeQueueService, 3, 3, TimeUnit.SECONDS);
        }

        pmemDataRecovery();
    }

    private void dataRecovery() throws IOException {

        long maxPhysicalOffset = consumeQueue.recover();

        commitLog.recover(maxPhysicalOffset);
    }

    public void pmemDataRecovery() throws IOException {
        indexHeap.load(getTopicQueueTable());
    }

    @Override
    public void stop() {
        this.commitLogProcessor.stop();
        consumeQueueSyncScheduledService.shutdownNow();
        try {
            if (!consumeQueueSyncScheduledService.awaitTermination(1, TimeUnit.SECONDS)) {
                log.warn("failed to stop consumeQueueSyncScheduledService");
            }
        } catch (InterruptedException e) {
            log.error("failed to stop consumeQueueSyncScheduledService", e);
        }
        this.consumeQueueService.stop();

        this.indexHeap.stop();
    }

    public long write(String topic, int queueId, ByteBuffer data) throws Exception {
        return storageSelector.selectWriter().write(topic, queueId, data);
    }

    public ByteBuffer getData(String topic, int queueId, long offset) throws Exception {
        return storageSelector.selectReader().getData(topic, queueId, offset);
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public TopicQueueTable getTopicQueueTable() {
        return consumeQueue.getTopicQueueTable();
    }

    public ConsumeQueue getConsumeQueue() {
        return consumeQueue;
    }

    public CommitLogProcessor getCommitLogProcessor() {
        return commitLogProcessor;
    }

    public PmemMsgStoreProcessor getPmemMsgStoreProcessor() {
        return pmemMsgStoreProcessor;
    }

    public IndexHeap getIndexHeap() {
        return indexHeap;
    }
}
