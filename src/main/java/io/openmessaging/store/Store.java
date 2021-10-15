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

public class Store implements StopWare {

    private static final Logger log = LoggerFactory.getLogger(Store.class);

    private final CommitLog commitLog;

    private final ConsumeQueue consumeQueue;

    private final Checkpoint checkpoint;

    private final ConsumeQueueService consumeQueueService;

    private final TopicQueueTable topicQueueTable;

    private final CommitLogProcessor commitLogProcessor;

    private final ScheduledExecutorService consumeQueueSyncScheduledService =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("consumeQueueSyncTask"));

    public Store() throws IOException {
        FileUtil.createDirIfNotExists(Config.getInstance().getRootDirPath());

        this.commitLog = new CommitLog(this);
        this.consumeQueue = new ConsumeQueue(this);
        this.checkpoint = new Checkpoint(this);
        this.consumeQueueService = new ConsumeQueueService(this);

        this.commitLogProcessor = new CommitLogProcessor(this);

        this.topicQueueTable = dataRecovery();

        // asyc write into consumeQueue
        if (Config.getInstance().isEnableConsumeQueueDataSync()) {
            consumeQueueSyncScheduledService
                    .scheduleAtFixedRate(consumeQueueService, 3, 3, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stop() {
        this.commitLogProcessor.stop();
        consumeQueueSyncScheduledService.shutdown();
        try {
            if (!consumeQueueSyncScheduledService.awaitTermination(1, TimeUnit.SECONDS)) {
                log.warn("failed to stop consumeQueueSyncScheduledService");
            }
        } catch (InterruptedException e) {
            log.error("failed to stop consumeQueueSyncScheduledService", e);
        }
        this.consumeQueueService.stop();
    }

    private TopicQueueTable dataRecovery() throws IOException {
        consumeQueue.syncFromCommitLog();
        return consumeQueue.loadTopicQueueTable();
    }

    public long write(String topic, int queueId, ByteBuffer data) throws IOException, InterruptedException {
        return commitLogProcessor.write(topic, queueId, data);
    }

    public ByteBuffer getData(String topic, int queueId, long offset) throws IOException {
        long commitLogOffset = topicQueueTable.getPhyOffset(topic, queueId, offset);
        if (commitLogOffset < 0) {
            return null;
        }
        return commitLog.getData(commitLogOffset);
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public ConsumeQueueService getConsumeQueueService() {
        return consumeQueueService;
    }

    public TopicQueueTable getTopicQueueTable() {
        return topicQueueTable;
    }

    public Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public ConsumeQueue getConsumeQueue() {
        return consumeQueue;
    }
}
