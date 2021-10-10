package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Store {

    private final CommitLog commitLog;

    private final ConsumerQueue consumerQueue;

    private final TopicQueueTable topicQueueTable;

    public Store() {
        this.consumerQueue = new ConsumerQueue(this);
        this.commitLog = new CommitLog(this);
        this.topicQueueTable = new TopicQueueTable();
    }

    public long write(String topic, int queueId, ByteBuffer data) throws IOException {
        int queueOffset = topicQueueTable.calcNextQueueOffset(topic, queueId);

        // sync write into commitLog
        long commitLogOffset = commitLog.write(topic, queueId, queueOffset, data);

        // write into consumerQueue
        return consumerQueue.write(topic, queueId, commitLogOffset);
    }

    public ByteBuffer getData(String topic, int queueId, long offset) throws IOException {
        long commitLogOffset = consumerQueue.findCommitLogOffset(topic, queueId, offset);
        if (commitLogOffset < 0) {
            return null;
        }
        return commitLog.getData(commitLogOffset);
    }
}
