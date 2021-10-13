package io.openmessaging.store;

import io.openmessaging.common.DoneFuture;

import java.nio.ByteBuffer;

public class Item {

    private final String topic;

    private final int queueId;

    private final ByteBuffer data;

    private long queueOffset;

    private long physicalOffset;

    // return queueOffset
    private final DoneFuture<Long> doneFuture;

    public Item(String topic, int queueId, ByteBuffer data) {
        this.topic = topic;
        this.queueId = queueId;
        this.data = data;
        this.doneFuture = new DoneFuture<>();
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

    public DoneFuture<Long> getDoneFuture() {
        return doneFuture;
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

