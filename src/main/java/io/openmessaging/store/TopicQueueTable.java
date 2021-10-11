package io.openmessaging.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author chenxi20
 * @date 2021/10/10
 */
public class TopicQueueTable {

    // topic-queueId-queueOffset -> phyOffset
    private final Map<String, Long> phyOffsets = new HashMap<>();

    // topic-queueId -> maxQueueOffset
    private final Map<String, Long> maxQueueOffsets = new HashMap<>();

    private String buildKey(String topic, int queueId, long queueOffset) {
        return topic + "_%_" + queueId + "_%_" + queueOffset;
    }

    private String buildKey(String topic, int queueId) {
        return topic + "_%_" + queueId;
    }

    public void put(String topic, int queueId, long queueOffset, long phyOffset) {
        maxQueueOffsets.put(buildKey(topic, queueId), queueOffset);
        phyOffsets.put(buildKey(topic, queueId, queueOffset), phyOffset);
    }

    public long getPhyOffset(String topic, int queueId, long queueOffset) {
        return phyOffsets.getOrDefault(buildKey(topic, queueId, queueOffset), -1L);
    }

    public long calcNextQueueOffset(String topic, int queueId) {
        return maxQueueOffsets.getOrDefault(buildKey(topic, queueId), -1L) + 1;
    }

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
