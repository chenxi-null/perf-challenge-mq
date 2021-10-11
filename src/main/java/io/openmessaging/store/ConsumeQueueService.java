package io.openmessaging.store;

import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
        long phyOffset = store.getCheckpoint().getPhyOffset();
        long commitLogWrotePosition = store.getCommitLog().readWrotePosition();
        while (phyOffset < commitLogWrotePosition) {
            try {
                CommitLog.TopicQueueOffsetInfo info = store.getCommitLog().getOffset(phyOffset);

                // consumeQueue write:
                //      (info.getTopic(), info.getQueueId(), info.getQueueOffset(), phyOffset);

                // update checkpoint
                store.getCheckpoint().updatePhyOffset(info.getNextPhyOffset());

                phyOffset = info.getNextPhyOffset();

            } catch (IOException e) {
                log.error("read commitLog occur error", e);
                Util.sleep(10_000);
            }
        }
    }
}
