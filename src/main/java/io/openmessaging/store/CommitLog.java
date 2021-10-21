package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.util.FileUtil;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * data struct
 * <p>
 * - msgSize
 * - body
 * - queueId
 * - queueOffset
 * - topic
 */
public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final Store store;

    private final FileChannel writeFileChannel;

    private final FileChannel readFileChannel;

    private final ByteBuffer wroteBuffer =
            ByteBuffer.allocateDirect(Config.getInstance().getBatchWriteCommitLogMaxDataSize());

    private volatile long wrotePosition = 0;

    public CommitLog(Store store) throws IOException {
        this.store = store;

        Path commitLogPath = Config.getInstance().getCommitLogPath();
        FileUtil.createFileIfNotExists(commitLogPath);

        this.writeFileChannel = new RandomAccessFile(commitLogPath.toFile(), "rw").getChannel();
        this.readFileChannel = writeFileChannel;
    }

    // sync invoke
    public void writeAndNotify(List<Item> items) throws IOException {
        if (items.isEmpty()) {
            return;
        }
        wroteBuffer.clear();

        long startPhysicalOffset = getWrotePosition();
        long physicalOffset = startPhysicalOffset;
        long nextPhysicalOffset = 0;
        int idx = 0;
        int itemSize = items.size();
        for (Item item : items) {
            long queueOffset = store.getTopicQueueTable().calcNextQueueOffset(item.getTopic(), item.getQueueId());
            item.setQueueOffset(queueOffset);

            item.setPhysicalOffset(physicalOffset);

            try {
                nextPhysicalOffset = appendByteBuffer(wroteBuffer, physicalOffset,
                        item.getTopic(), item.getQueueId(), queueOffset, item.getData());
            } catch (Throwable e) {
                log.error("failed to appendByteBuffer, physicalOffset: {}, topic: {}, queueId: {}, queueOffset: {}, "
                                + "wroteBuffer: {}, data: {} | idx: {}, itemSize: {}",
                        physicalOffset, item.getTopic(), item.getQueueId(), queueOffset,
                        wroteBuffer, item.getData(),
                        idx, itemSize, e);

                // async throw exception in order to fast-fail
                item.getDoneFuture().done(null);

                throw e;
            }
            physicalOffset = nextPhysicalOffset;
            ++idx;
        }

        wroteBuffer.flip();
        writeFileChannel.position(startPhysicalOffset);
        writeFileChannel.write(wroteBuffer);
        writeFileChannel.force(true);

        for (Item item : items) {
            updateTopicQueueTable(item.getTopic(), item.getQueueId(), item.getQueueOffset(), item.getPhysicalOffset());
            //notify
            item.getDoneFuture().done(item.getQueueOffset());
        }

        setWrotePosition(nextPhysicalOffset);
        log.debug("wrote, physicalOffset: {}, nextPhysicalOffset: {}", startPhysicalOffset, nextPhysicalOffset);
    }

    //----------------------------------------------------

    // TODO:
    private static final int logSizeBytesNum = 4;

    private static final int msgSizeBytesNum = 4;

    /**
     * @return nextPhysicalOffset
     */
    public long appendByteBuffer(ByteBuffer byteBuffer, long physicalOffset,
                                 String topic, int queueId, long queueOffset, ByteBuffer data) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.remaining();
        Util.assertTrue(msgSize <= Config.getInstance().getOneWriteMaxDataSize(), "unexpected msgSize: " + msgSize);
        int bufferSize = logSizeBytesNum /* logSize */
                + msgSizeBytesNum /* msgSize */
                + msgSize /* data */
                + 4 /* queueId */
                + 8 /* queueOffset */
                + topicBytes.length;
        byteBuffer.putInt(bufferSize);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(queueOffset);
        byteBuffer.put(topicBytes);

        return physicalOffset + bufferSize;
    }

    private void updateTopicQueueTable(String topic, int queueId, long queueOffset, long physicalOffset) {
        store.getTopicQueueTable().put(topic, queueId, queueOffset, physicalOffset);
    }

    public ByteBuffer getData(long physicalOffset) throws IOException {
        int msgSize = -1;
        ByteBuffer buffer = null;
        try {
            msgSize = readMsgSize(physicalOffset);

            buffer = bufferContext.get().getMsgDataBuffer();
            buffer.clear();
            buffer.limit(msgSize);
            readFileChannel.read(buffer, physicalOffset + 4 + 4);
            return buffer;
        } catch (Throwable e) {
            log.error("physicalOffset: {}, msgSize: {}, buffer: {}", physicalOffset, msgSize, buffer, e);
            throw e;
        }
    }

    //+ 4 /* logSize */
    //+ 4 /* msgSize */
    //+ msgSize
    //+ 4 /* queueId */
    //+ 8 /* queueOffset */
    //+ topicBytes.length;

    // return: topic, queueId, queueOffset and nextPhyOffset
    public TopicQueueOffsetInfo getLogicItemInfo(long phyOffset, ByteBuffer prefixSizeBuffer,
                                                 ByteBuffer suffixBuffer, byte[] suffixBytes) throws IOException {
        SizeInfo sizeInfo = readSize(phyOffset, prefixSizeBuffer);
        int logSize = sizeInfo.logSize;
        int msgSize = sizeInfo.msgSize;

        suffixBuffer.clear();
        int capacity = logSize - 4 - 4 - msgSize;
        suffixBuffer.limit(capacity);
        readFileChannel.read(suffixBuffer, phyOffset + 4 + 4 + msgSize);

        suffixBuffer.flip();
        int queueId = suffixBuffer.getInt();
        long queueOffset = suffixBuffer.getLong();

        int topicBytesNum = capacity - 4 - 8;
        byte[] topicBytes = new byte[topicBytesNum];
        suffixBuffer.get(topicBytes);
        String topic = new String(topicBytes, 0, topicBytesNum, StandardCharsets.ISO_8859_1);

        long nextPhyOffset = phyOffset + logSize;
        return new TopicQueueOffsetInfo(topic, queueId, queueOffset, nextPhyOffset);
    }

    public void recover(long maxPhysicalOffset) {
        //      check data item by crc
        //      update wrotePosition
        //      update mem topicQueueTable
    }

    public static class TopicQueueOffsetInfo {
        String topic;
        int queueId;
        long queueOffset;
        long nextPhyOffset;

        public TopicQueueOffsetInfo(String topic, int queueId, long queueOffset, long nextPhyOffset) {
            this.topic = topic;
            this.queueId = queueId;
            this.queueOffset = queueOffset;
            this.nextPhyOffset = nextPhyOffset;
        }

        @Override
        public String toString() {
            return "TopicQueueOffsetInfo{" +
                    "topic='" + topic + '\'' +
                    ", queueId=" + queueId +
                    ", queueOffset=" + queueOffset +
                    ", nextPhyOffset=" + nextPhyOffset +
                    '}';
        }

        public String getTopic() {
            return topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public long getQueueOffset() {
            return queueOffset;
        }

        public long getNextPhyOffset() {
            return nextPhyOffset;
        }
    }

    // return: logSize and msgSize
    private SizeInfo readSize(long physicalOffset, ByteBuffer prefixSizeBuffer) throws IOException {
        prefixSizeBuffer.clear();
        readFileChannel.read(prefixSizeBuffer, physicalOffset);
        prefixSizeBuffer.flip();
        return new SizeInfo(prefixSizeBuffer.getInt(), prefixSizeBuffer.getInt());
    }

    private int readMsgSize(long physicalOffset) throws IOException {
        ByteBuffer msgSizeBuffer = bufferContext.get().getMsgSizeBuffer();
        msgSizeBuffer.clear();
        readFileChannel.read(msgSizeBuffer, physicalOffset + 4);
        msgSizeBuffer.flip();
        return msgSizeBuffer.getInt();
    }


    //polish
    public static final ThreadLocal<Buffers> bufferContext = ThreadLocal.withInitial(Buffers::new);

    static class Buffers {
        private final ByteBuffer msgSizeBuffer = ByteBuffer.allocateDirect(4);

        private final ConcurrentHashMap<Integer, ByteBuffer> map = new ConcurrentHashMap<>();

        public ByteBuffer getMsgSizeBuffer() {
            return msgSizeBuffer;
        }

        public ByteBuffer getMsgDataBuffer() {
            Integer queryIdx = DefaultMessageQueueImpl.queryIdxContext.get();
            return map.computeIfAbsent(queryIdx, k -> ByteBuffer.allocateDirect(17 * 1024));
        }
    }

    static class SizeInfo {
        int logSize;
        int msgSize;

        public SizeInfo(int logSize, int msgSize) {
            this.logSize = logSize;
            this.msgSize = msgSize;
        }
    }

    public void setWrotePosition(long wrotePosition) {
        this.wrotePosition = wrotePosition;
    }

    public long getWrotePosition() {
        return wrotePosition;
    }
}
