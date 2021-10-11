package io.openmessaging;

import io.openmessaging.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    private final Store store;

    public DefaultMessageQueueImpl() {
        try {
            this.store = new Store();
            init();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() throws IOException {
        if (!Files.exists(Config.getInstance().getCommitLogPath())) {
            Files.createFile(Config.getInstance().getCommitLogPath());
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        log.info("mq append, ({}, {}), size: {}", topic, queueId, data);
        try {
            return store.write(topic, queueId, data);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long startOffset, int fetchNum) {
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
        return map;
    }

    public Store getStore() {
        return store;
    }
}
