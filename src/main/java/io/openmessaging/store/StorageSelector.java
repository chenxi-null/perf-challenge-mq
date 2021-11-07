package io.openmessaging.store;

import io.openmessaging.Config;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenxi20
 * @date 2021/10/28
 */
public class StorageSelector {

    private final Store store;

    private final MixedMsgStoreReader mixedMsgStoreReader;

    private final SsdMsgStoreReader ssdMsgStoreReader;

    private final AtomicInteger writeCounter = new AtomicInteger();

    private final Config config = Config.getInstance();

    public StorageSelector(Store store) {
        this.store = store;
        mixedMsgStoreReader = new MixedMsgStoreReader(store.getPmemMsgStoreProcessor(), store.getCommitLogProcessor());
        ssdMsgStoreReader = new SsdMsgStoreReader(store.getCommitLogProcessor());
    }

    public MsgStoreProcessor selectWriter() {
        if (config.isEnablePmem()) {
            writeCounter.incrementAndGet();
            if (writeCounter.get() % 2 == 0) {
                return store.getCommitLogProcessor();
            } else {
                return store.getPmemMsgStoreProcessor();
            }
        } else {
            return store.getCommitLogProcessor();
        }
    }

    public MsgStoreProcessor selectReader() {
        if (config.isEnablePmem()) {
            return mixedMsgStoreReader;
        } else {
            return ssdMsgStoreReader;
        }
    }

    private static class SsdMsgStoreReader implements MsgStoreProcessor {

        private final CommitLogProcessor commitLogProcessor;

        public SsdMsgStoreReader(CommitLogProcessor commitLogProcessor) {
            this.commitLogProcessor = commitLogProcessor;
        }

        @Override
        public long write(String topic, int queueId, ByteBuffer data) throws Exception {
            throw new UnsupportedOperationException("unexpected write opt");
        }

        @Override
        public ByteBuffer getData(String topic, int queueId, long offset) throws Exception {
            return commitLogProcessor.getData(topic, queueId, offset);
        }
    }

    private static class MixedMsgStoreReader implements MsgStoreProcessor {

        private final PmemMsgStoreProcessor pmemMsgStoreProcessor;

        private final CommitLogProcessor commitLogProcessor;

        public MixedMsgStoreReader(PmemMsgStoreProcessor pmemMsgStoreProcessor, CommitLogProcessor commitLogProcessor) {
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
