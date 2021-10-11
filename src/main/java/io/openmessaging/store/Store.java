package io.openmessaging.store;

import io.openmessaging.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Store {

    private final CommitLog commitLog;

    private final ConsumeQueue consumeQueue;

    private final Checkpoint checkpoint;

    private final ConsumeQueueService consumeQueueService;

    private final TopicQueueTable topicQueueTable;

    private final ReentrantLock writeLock = new ReentrantLock();

    public Store() throws IOException {
        this.commitLog = new CommitLog(this);
        this.consumeQueue = new ConsumeQueue(this);
        this.checkpoint = new Checkpoint();
        this.consumeQueueService = new ConsumeQueueService(this);

        this.topicQueueTable = dataRecovery();

        // asyc write into consumeQueue
        if (Config.getInstance().isEnableConsumeQueueDataSync()) {
            Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(consumeQueueService, 0, 3, TimeUnit.SECONDS);
        }
    }

    private TopicQueueTable dataRecovery() throws IOException {
        consumeQueue.syncFromCommitLog();
        return consumeQueue.loadTopicQueueTable();
    }

    public long write(String topic, int queueId, ByteBuffer data) throws IOException {
        writeLock.lock();
        try {
            long queueOffset = topicQueueTable.calcNextQueueOffset(topic, queueId);
            // sync write into commitLog
            commitLog.write(topic, queueId, queueOffset, data);
            return queueOffset;
        } finally {
            writeLock.unlock();
        }
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
