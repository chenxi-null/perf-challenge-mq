package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.common.NamedThreadFactory;
import io.openmessaging.common.StopWare;
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
public class Store implements StopWare {

    private static final Logger log = LoggerFactory.getLogger(Store.class);

    private final CommitLog commitLog;

    private final ConsumeQueue consumeQueue;

    private final ConsumeQueueService consumeQueueService;

    private final CommitLogProcessor commitLogProcessor;

    private final ScheduledExecutorService consumeQueueSyncScheduledService =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("consumeQueueSyncTask"));

    public Store() throws IOException {
        FileUtil.createDirIfNotExists(Config.getInstance().getRootDirPath());

        this.commitLog = new CommitLog(this);
        this.consumeQueue = new ConsumeQueue(this);
        this.consumeQueueService = new ConsumeQueueService(this);

        this.commitLogProcessor = new CommitLogProcessor(this);

        start();
    }

    public void start() throws IOException {
        dataRecovery();

        consumeQueue.syncFromCommitLog();

        // asyc write into consumeQueue
        if (Config.getInstance().isEnableConsumeQueueDataSync()) {
            consumeQueueSyncScheduledService
                    .scheduleAtFixedRate(consumeQueueService, 3, 3, TimeUnit.SECONDS);
        }
    }

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
    //  |---------------|------------------|
    //                 cq               log, memTable
    //
    private void dataRecovery() throws IOException {

        long maxPhysicalOffset = consumeQueue.recover();

        commitLog.recover(maxPhysicalOffset);
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
    }

    public long write(String topic, int queueId, ByteBuffer data) throws IOException, InterruptedException {
        return commitLogProcessor.write(topic, queueId, data);
    }

    public ByteBuffer getData(String topic, int queueId, long offset) throws IOException {
        long commitLogOffset = getTopicQueueTable().getPhyOffset(topic, queueId, offset);
        if (commitLogOffset < 0) {
            return null;
        }
        return commitLog.getData(commitLogOffset);
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
}
