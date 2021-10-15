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

    private final AtomicLong wroteNum = new AtomicLong();

    private final AtomicLong capacityStat = new AtomicLong();

    private final AtomicLong queryTimeStats = new AtomicLong();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        long startTime = System.currentTimeMillis();
        long wroteBytes = capacityStat.addAndGet(data.capacity());
        //log.info("mq append, ({}, {}), dataSize: {}, wroteBytes: {}", topic, queueId, data.capacity(), wroteBytes);
        try {
            long queueOffset = store.write(topic, queueId, data);

            long endTime = System.currentTimeMillis();
            long costTime = endTime - startTime;
            long wroteNum = this.wroteNum.getAndIncrement();
            if (wroteNum % 100 == 0) {
                log.info("finish mq append, idx = {}, cost = {}, totalCost = {}, ({}, {}), dataSize: {}, wroteBytes: {}",
                        wroteNum, costTime, queryTimeStats.addAndGet(costTime),
                        topic, queueId, data.capacity(), wroteBytes);
            }

            return queueOffset;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long startOffset, int fetchNum) {
        long startTime = System.currentTimeMillis();
        //log.info("mq getRange, ({}, {}), {}, {}", topic, queueId, startOffset, fetchNum);
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
        long wroteNum = this.wroteNum.getAndIncrement();
        if (wroteNum % 100 == 0) {
            log.info("finish mq getRange, idx = {}, cost = {}, totalCost = {}, ({}, {}), {}, {}",
                    wroteNum, costTime, queryTimeStats.addAndGet(costTime),
                    topic, queueId, startOffset, fetchNum);
        }
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
