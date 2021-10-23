package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.util.ChecksumUtil;
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

public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final Store store;

    private final FileChannel fileChannel;

    private final ByteBuffer wroteBuffer =
            ByteBuffer.allocateDirect(Config.getInstance().getBatchWriteCommitLogMaxDataSize());

    private volatile long wrotePosition = 0;

    public CommitLog(Store store) throws IOException {
        this.store = store;

        Path commitLogPath = Config.getInstance().getCommitLogPath();
        FileUtil.createFileIfNotExists(commitLogPath);

        this.fileChannel = new RandomAccessFile(commitLogPath.toFile(), "rw").getChannel();
    }

    public void recover(long maxPhysicalOffset) {
        // TODO:
        //      check data item by crc
        //      update wrotePosition
        //      update mem topicQueueTable
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
        fileChannel.position(startPhysicalOffset);
        fileChannel.write(wroteBuffer);
        fileChannel.force(true);

        for (Item item : items) {
            updateTopicQueueTable(item.getTopic(), item.getQueueId(), item.getQueueOffset(), item.getPhysicalOffset());
            //notify
            item.getDoneFuture().done(item.getQueueOffset());
        }

        setWrotePosition(nextPhysicalOffset);
        log.debug("wrote, physicalOffset: {}, nextPhysicalOffset: {}", startPhysicalOffset, nextPhysicalOffset);
    }

    //----------------------------------------------------

    /**
     * @return nextPhysicalOffset
     */
    public long appendByteBuffer(ByteBuffer byteBuffer, long physicalOffset,
                                 String topic, int queueId, long queueOffset, ByteBuffer data) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.remaining();
        Util.assertTrue(msgSize <= Config.getInstance().getOneWriteMaxDataSize(),
                "unexpected msgSize: " + msgSize);
        int logSize = 4 /* logSize */
                + 4 /* msgSize */
                + msgSize /* data */
                + 4 /* queueId */
                + 8 /* queueOffset */
                + topicBytes.length
                + 4 /* checksum */;
        byteBuffer.putInt(logSize);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(queueOffset);
        byteBuffer.put(topicBytes);
        int checksum = ChecksumUtil.get(byteBuffer.array(), 0, byteBuffer.position());
        byteBuffer.putInt(checksum);

        return physicalOffset + logSize;
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
            fileChannel.read(buffer, physicalOffset + 4 + 4);
            return buffer;
        } catch (Throwable e) {
            log.error("physicalOffset: {}, msgSize: {}, buffer: {}", physicalOffset, msgSize, buffer, e);
            throw e;
        }
    }

    // polish: log item content, can remove some content
    //+ 4 /* logSize */
    //+ 4 /* msgSize */
    //+ msgSize
    //+ 4 /* queueId */
    //+ 8 /* queueOffset */
    //+ topicBytes.length;
    public LogicItemInfo getLogicItemInfo(long phyOffset, ByteBuffer prefixSizeBuffer,
                                          ByteBuffer suffixBuffer, byte[] suffixBytes) throws IOException {
        SizeInfo sizeInfo = readSize(phyOffset, prefixSizeBuffer);
        int logSize = sizeInfo.logSize;
        int msgSize = sizeInfo.msgSize;

        suffixBuffer.clear();
        int capacity = logSize - 4 - 4 - msgSize;
        suffixBuffer.limit(capacity);
        fileChannel.read(suffixBuffer, phyOffset + 4 + 4 + msgSize);

        suffixBuffer.flip();
        int queueId = suffixBuffer.getInt();
        long queueOffset = suffixBuffer.getLong();

        int topicBytesNum = capacity - 4 - 8;
        byte[] topicBytes = new byte[topicBytesNum];
        suffixBuffer.get(topicBytes);
        String topic = new String(topicBytes, 0, topicBytesNum, StandardCharsets.ISO_8859_1);

        long nextPhyOffset = phyOffset + logSize;
        return new LogicItemInfo(topic, queueId, queueOffset, nextPhyOffset);
    }

    public static class LogicItemInfo {
        String topic;
        int queueId;
        long queueOffset;
        long nextPhyOffset;

        public LogicItemInfo(String topic, int queueId, long queueOffset, long nextPhyOffset) {
            this.topic = topic;
            this.queueId = queueId;
            this.queueOffset = queueOffset;
            this.nextPhyOffset = nextPhyOffset;
        }

        @Override
        public String toString() {
            return "LogicItemInfo{" +
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

    /**
     * @return logSize and msgSize
     */
    private SizeInfo readSize(long physicalOffset, ByteBuffer prefixSizeBuffer) throws IOException {
        prefixSizeBuffer.clear();
        fileChannel.read(prefixSizeBuffer, physicalOffset);
        prefixSizeBuffer.flip();
        return new SizeInfo(prefixSizeBuffer.getInt(), prefixSizeBuffer.getInt());
    }

    private int readMsgSize(long physicalOffset) throws IOException {
        ByteBuffer msgSizeBuffer = bufferContext.get().getMsgSizeBuffer();
        msgSizeBuffer.clear();
        fileChannel.read(msgSizeBuffer, physicalOffset + 4);
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
