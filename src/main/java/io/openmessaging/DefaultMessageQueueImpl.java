package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue {

    private final Store store;

    public DefaultMessageQueueImpl() {
        this.store = new Store();
        try {
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
        try {
            return store.write(topic, queueId, data);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long startOffset, int fetchNum) {
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
}
