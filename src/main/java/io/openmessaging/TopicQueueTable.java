package io.openmessaging;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenxi20
 * @date 2021/10/10
 */
public class TopicQueueTable {
    // key: topic-queue
    private final Map<String, Offset> map = new HashMap<>();

    private String buildKey(String topic, int queueId) {
        return topic + "_%_" + queueId;
    }

    Offset get(String topic, int queueId) {
        return map.get(buildKey(topic, queueId));
    }

    int calcNextQueueOffset(String topic, int queueId) {
        Offset offset = get(topic, queueId);
        if (offset == null) {
            return 0;
        }
        return offset.queueOffset + 1;
    }

    public static class Offset {
        int queueOffset;
        long phyOffset;

        public Offset(int queueOffset, long phyOffset) {
            this.queueOffset = queueOffset;
            this.phyOffset = phyOffset;
        }
    }
}
