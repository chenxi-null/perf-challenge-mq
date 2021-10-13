package io.openmessaging.store;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class Item {

    private final String topic;

    private final int queueId;

    private final ByteBuffer data;

    private long queueOffset;

    private long physicalOffset;

    // return queueOffset
    Future<Long> future;

    public Item(String topic, int queueId, ByteBuffer data) {
        this.topic = topic;
        this.queueId = queueId;
        this.data = data;

        // TODO: init future
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public ByteBuffer getData() {
        return data;
    }

    public Future<Long> getFuture() {
        return future;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }
}

