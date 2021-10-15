package io.openmessaging;

import io.openmessaging.common.StopWare;
import io.openmessaging.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMessageQueueImpl extends MessageQueue implements StopWare {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    private final Store store;

    public DefaultMessageQueueImpl() {
        try {
            this.store = new Store();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicLong capacityStat = new AtomicLong();

    private final AtomicLong queryTimeStats = new AtomicLong();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        long wroteBytes = capacityStat.addAndGet(data.capacity());
        log.info("mq append, ({}, {}), dataSize: {}, wroteBytes: {}", topic, queueId, data.capacity(), wroteBytes);
        try {
            return store.write(topic, queueId, data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long startOffset, int fetchNum) {
        long startTime = System.currentTimeMillis();
        log.info("mq getRange, ({}, {}), {}, {}", topic, queueId, startOffset, fetchNum);
        Map<Integer, ByteBuffer> map = new HashMap<>();
        for (int i = 0; i < fetchNum; i++) {
            long offset = startOffset + i;
            ByteBuffer data;
            try {
                data = store.getData(topic, queueId, offset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (data != null) {
                map.put(i, data);
            }
        }
        long endTime = System.currentTimeMillis();
        long costTime = endTime - startTime;
        log.info("finish mq getRange, cost = {}, ({}, {}), {}, {}",
                costTime, topic, queueId, startOffset, fetchNum);
        queryTimeStats.addAndGet(costTime);
        return map;
    }

    public Store getStore() {
        return store;
    }

    @Override
    public void stop() {
        store.stop();
    }
}
