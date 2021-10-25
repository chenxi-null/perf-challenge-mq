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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final Store store;

    private final FileChannel fileChannel;

    private volatile long wrotePosition = 0;

    private final ByteBuffer wroteBuffer =
            ByteBuffer.allocateDirect(Config.getInstance().getBatchWriteCommitLogMaxDataSize());

    private final ByteBuffer itemBuffer =
            ByteBuffer.allocate(200 + Config.getInstance().getOneWriteMaxDataSize());

    public CommitLog(Store store) throws IOException {
        this.store = store;

        Path commitLogPath = Config.getInstance().getCommitLogPath();
        FileUtil.createFileIfNotExists(commitLogPath);

        this.fileChannel = new RandomAccessFile(commitLogPath.toFile(), "rw").getChannel();
    }

    // - check data item by checksum
    // - update mem topicQueueTable
    // - update wrotePosition
    //
    // 4 /* logSize */
    // 4 /* msgSize */
    // msgSize /* data */
    // 4 /* queueId */
    // 8 /* queueOffset */
    // topicBytes.length
    // 4 /* checksum */;
    //
    public void recover(long maxPhysicalOffset) throws IOException {
        log.info("recover, maxPhysicalOffset: {}", maxPhysicalOffset);

        byte[] topicBytes = new byte[Config.getInstance().getTopicMaxByteNum()];
        byte[] logItemBytes = new byte[Config.getInstance().getLogItemMaxByteNum()];
        long phyOffset = maxPhysicalOffset;
        while (true) {
            fileChannel.position(phyOffset);
            itemBuffer.clear();
            int readBytes = fileChannel.read(itemBuffer);
            if (readBytes <= 0) {
                break;
            }

            itemBuffer.flip();
            int logSize = itemBuffer.getInt();
            int msgSize = itemBuffer.getInt();
            itemBuffer.position(itemBuffer.position() + msgSize);
            int queueId = itemBuffer.getInt();
            long queueOffset = itemBuffer.getLong();

            int topicLength = logSize - 4 - 4 - msgSize - 4 - 8 - 4;
            itemBuffer.get(topicBytes, 0, topicLength);
            String topic = new String(topicBytes, 0, topicLength, StandardCharsets.UTF_8);

            int checksum = itemBuffer.getInt();

            itemBuffer.rewind();
            itemBuffer.get(logItemBytes, 0, logSize - 4);
            int expectedChecksum = ChecksumUtil.get(logItemBytes, 0, logSize - 4);

            if (!Objects.equals(checksum, expectedChecksum)) {
                log.warn("find dirty tail data");
                break;
            }

            this.store.getTopicQueueTable().put(topic, queueId, queueOffset, phyOffset);

            phyOffset += logSize;
        }

        setWrotePosition(phyOffset);
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

    /**
     * @return nextPhysicalOffset
     */
    public long appendByteBuffer(ByteBuffer byteBuffer, long physicalOffset,
                                 String topic, int queueId, long queueOffset, ByteBuffer data) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        int startPosition = byteBuffer.position();

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

        int pos = byteBuffer.position();

        // make buffer ready to get
        byteBuffer.limit(pos);
        byteBuffer.position(startPosition);
        byte[] bytes = new byte[logSize - 4];
        byteBuffer.get(bytes, 0, logSize - 4);
        int checksum = ChecksumUtil.get(bytes, 0, bytes.length);

        // restore buffer, make it ready to put again
        byteBuffer.clear();
        byteBuffer.position(pos);
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

    //+ 4 /* logSize */
    //+ 4 /* msgSize */
    //+ msgSize
    //+ 4 /* queueId */
    //+ 8 /* queueOffset */
    //+ topicBytes.length;
    //+ 4 /* checksum */
    public LogicItemInfo getLogicItemInfo(long phyOffset, ByteBuffer itemHeadSizeBuffer,
                                          ByteBuffer itemTailBuffer) throws IOException {
        SizeInfo sizeInfo = readSize(phyOffset, itemHeadSizeBuffer);
        int logSize = sizeInfo.logSize;
        int msgSize = sizeInfo.msgSize;

        itemTailBuffer.clear();
        int capacity = logSize - 4 - 4 - msgSize;
        itemTailBuffer.limit(capacity);
        fileChannel.read(itemTailBuffer, phyOffset + 4 + 4 + msgSize);

        itemTailBuffer.flip();
        int queueId = itemTailBuffer.getInt();
        long queueOffset = itemTailBuffer.getLong();

        int topicBytesNum = capacity - 4 - 8 - 4;
        //polish: reuse this byte array
        byte[] topicBytes = new byte[topicBytesNum];
        itemTailBuffer.get(topicBytes);
        String topic = new String(topicBytes, 0, topicBytesNum, StandardCharsets.UTF_8);

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
