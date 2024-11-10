package io.openmessaging.store;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chenxi
 * @date 2021/10/10
 */
public class TopicQueueTable {

    // topic-queueId, queueOffset -> pmemOffset
    private final Map<String, Long> pmemOffsets = new ConcurrentHashMap<>();

    // topic-queueId, queueOffset -> phyOffset
    private final Map<String, Long> phyOffsets = new ConcurrentHashMap<>();

    // topic-queueId -> maxQueueOffset
    private final Map<String, Long> maxQueueOffsets = new ConcurrentHashMap<>();

    private final ReentrantLock wroteLock = new ReentrantLock();

    private String buildKey(String topic, int queueId, long queueOffset) {
        return topic + "_%_" + queueId + "_%_" + queueOffset;
    }

    private String buildKey(String topic, int queueId) {
        return topic + "_%_" + queueId;
    }

    public void put(String topic, int queueId, long queueOffset, long phyOffset) {
        wroteLock.lock();
        try {
            maxQueueOffsets.put(buildKey(topic, queueId), queueOffset);
            phyOffsets.put(buildKey(topic, queueId, queueOffset), phyOffset);
        } finally {
            wroteLock.unlock();
        }
    }

    public void putByPmem(String topic, int queueId, long queueOffset, long pmemOffset) {
        wroteLock.lock();
        try {
            maxQueueOffsets.put(buildKey(topic, queueId), queueOffset);
            pmemOffsets.put(buildKey(topic, queueId, queueOffset), pmemOffset);
        } finally {
            wroteLock.unlock();
        }
    }

    public long getPmemOffset(String topic, int queueId, long queueOffset) {
        return pmemOffsets.getOrDefault(buildKey(topic, queueId, queueOffset), -1L);
    }

    public long getPhyOffset(String topic, int queueId, long queueOffset) {
        return phyOffsets.getOrDefault(buildKey(topic, queueId, queueOffset), -1L);
    }

    public long calcNextQueueOffset(String topic, int queueId) {
        return maxQueueOffsets.getOrDefault(buildKey(topic, queueId), -1L) + 1;
    }

    @Override
    public String toString() {
        return "TopicQueueTable{\n" +
                " pmemOffsets=" + pmemOffsets +
                ",\n phyOffsets=" + phyOffsets +
                ",\n maxQueueOffsets=" + maxQueueOffsets +
                '}';
    }

    // ----------------------------------------------------

    // for test
    public long getMsgNum() {
        long sum = 0;
        for (Long num : maxQueueOffsets.values()) {
            if (num != null) {
                sum += num + 1;
            }
        }
        return sum;
    }

    // for test
    public boolean isSame(TopicQueueTable other) {
        return Objects.equals(this.phyOffsets, other.phyOffsets)
                && Objects.equals(this.maxQueueOffsets, other.maxQueueOffsets);
    }
}
