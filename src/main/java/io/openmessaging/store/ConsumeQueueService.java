package io.openmessaging.store;

import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author chenxi20
 * @date 2021/10/10
 */
public class ConsumeQueueService implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueue.class);

    private final Store store;

    public ConsumeQueueService(Store store) {
        this.store = store;
    }

    @Override
    public void run() {
        ByteBuffer data = null;
        do {
            try {
                data = store.getCommitLog().getData(store.getCheckpoint().getPhyOffset());
            } catch (IOException e) {
                log.error("read commitLog occur error", e);
                Util.sleep(10_000);
            }
        } while (data != null);
    }
}
