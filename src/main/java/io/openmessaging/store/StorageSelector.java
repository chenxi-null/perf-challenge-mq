package io.openmessaging.store;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenxi20
 * @date 2021/10/28
 */
public class StorageSelector {

    private final Store store;

    private final MsgStoreReader msgStoreReader;

    public StorageSelector(Store store) {
        this.store = store;
        msgStoreReader = new MsgStoreReader(store.getPmemMsgStoreProcessor(), store.getCommitLogProcessor());
    }

    private AtomicInteger writeCounter = new AtomicInteger();

    MsgStoreProcessor selectWriter() {
        writeCounter.incrementAndGet();
        if (writeCounter.get() % 2 == 0) {
            return store.getCommitLogProcessor();
        } else {
            return store.getPmemMsgStoreProcessor();
        }
    }

    MsgStoreProcessor selectReader() {
        return msgStoreReader;
    }

    static class MsgStoreReader implements MsgStoreProcessor {

        private final PmemMsgStoreProcessor pmemMsgStoreProcessor;

        private final CommitLogProcessor commitLogProcessor;

        public MsgStoreReader(PmemMsgStoreProcessor pmemMsgStoreProcessor, CommitLogProcessor commitLogProcessor) {
            this.pmemMsgStoreProcessor = pmemMsgStoreProcessor;
            this.commitLogProcessor = commitLogProcessor;
        }

        @Override
        public long write(String topic, int queueId, ByteBuffer data) throws Exception {
            throw new UnsupportedOperationException("unexpected write opt");
        }

        @Override
        public ByteBuffer getData(String topic, int queueId, long offset) throws Exception {
            ByteBuffer pmemData = pmemMsgStoreProcessor.getData(topic, queueId, offset);
            if (pmemData != null) {
                return pmemData;
            }
            return commitLogProcessor.getData(topic, queueId, offset);
        }
    }
}
