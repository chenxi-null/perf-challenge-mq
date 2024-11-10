package io.openmessaging.store;

import io.openmessaging.common.StopWare;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author chenxi
 * @date 2021/10/10
 */
public class ConsumeQueueService implements Runnable, StopWare {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueueService.class);

    private final Store store;

    public ConsumeQueueService(Store store) {
        this.store = store;
    }

    @Override
    public void run() {
        try {
            store.getConsumeQueue().syncFromCommitLog();
        } catch (IOException e) {
            log.error("read commitLog occur error", e);
            Util.sleep(1_000);
        }
    }

    @Override
    public void stop() {
        store.getConsumeQueue().stop();
    }
}
