package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.util.FileUtil;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

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

    private final ByteBuffer wrotePositionBuffer;

    private final ByteBuffer wroteBuffer =
            ByteBuffer.allocate(Config.getInstance().getBatchWriteCommitLogMaxDataSize());

    public CommitLog(Store store) throws IOException {
        this.store = store;

        Path commitLogPath = Config.getInstance().getCommitLogPath();
        FileUtil.createFileIfNotExists(commitLogPath);

        this.writeFileChannel = FileChannel.open(commitLogPath,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.readFileChannel = FileChannel.open(commitLogPath,
                StandardOpenOption.READ);

        wrotePositionBuffer = ByteBuffer.allocate(8);
        updateWrotePosition(8);
    }

    public int getInitWrotePosition() {
        return 8;
    }

    public void writeAndNotify(List<Item> items) throws IOException {
        if (items.isEmpty()) {
            return;
        }
        wroteBuffer.clear();

        long startPhysicalOffset = readWrotePosition();
        long physicalOffset = startPhysicalOffset;
        long nextPhysicalOffset = 0;
        for (Item item : items) {
            long queueOffset = store.getTopicQueueTable().calcNextQueueOffset(item.getTopic(), item.getQueueId());
            item.setQueueOffset(queueOffset);

            item.setPhysicalOffset(physicalOffset);

            nextPhysicalOffset = appendByteBuffer(wroteBuffer, physicalOffset,
                    item.getTopic(), item.getQueueId(), queueOffset, item.getData());
            physicalOffset = nextPhysicalOffset;
        }

        wroteBuffer.flip();
        writeFileChannel.write(wroteBuffer);
        writeFileChannel.force(true);

        updateWrotePosition(nextPhysicalOffset);
        log.info("wrote, physicalOffset: {}, nextPhysicalOffset: {}", startPhysicalOffset, nextPhysicalOffset);

        for (Item item :  items) {
            updateTopicQueueTable(item.getTopic(), item.getQueueId(), item.getQueueOffset(), item.getPhysicalOffset());
            //notify
            item.getDoneFuture().done(item.getQueueOffset());
        }
    }

    /**
     * @return nextPhysicalOffset
     */
    public long appendByteBuffer(ByteBuffer byteBuffer, long physicalOffset,
                                 String topic, int queueId, long queueOffset, ByteBuffer data) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.capacity();
        int bufferSize = 4 /* logSize */
                + 4 /* msgSize */ + msgSize
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

    public long readWrotePosition() throws IOException {
        wrotePositionBuffer.clear();
        int readBytes = readFileChannel.read(wrotePositionBuffer, 0);
        Util.assertTrue(readBytes == 8);
        wrotePositionBuffer.flip();
        return wrotePositionBuffer.getLong();
    }

    public void updateWrotePosition(long nextPhyOffset) throws IOException {
        log.info("updateWrotePosition: " + nextPhyOffset);
        wrotePositionBuffer.clear();
        wrotePositionBuffer.putLong(nextPhyOffset);
        wrotePositionBuffer.flip();
        writeFileChannel.write(wrotePositionBuffer, 0);
        writeFileChannel.force(true);
    }

    private void updateTopicQueueTable(String topic, int queueId, long queueOffset, long physicalOffset) {
        store.getTopicQueueTable().put(topic, queueId, queueOffset, physicalOffset);
    }

    public ByteBuffer getData(long physicalOffset) throws IOException {
        int msgSize = readMsgSize(physicalOffset);

        ByteBuffer buffer = ByteBuffer.allocate(msgSize);
        buffer.clear();
        readFileChannel.read(buffer, physicalOffset + 4 + 4);
        return buffer;
    }

    //+ 4 /* logSize */
    //+ 4 /* msgSize */ + msgSize
    //+ 4 /* queueId */
    //+ 8 /* queueOffset */
    //+ topicBytes.length;

    // return: topic, queueId, queueOffset and nextPhyOffset
    public TopicQueueOffsetInfo getOffset(long phyOffset) throws IOException {
        SizeInfo sizeInfo = readSize(phyOffset);
        int logSize = sizeInfo.logSize;
        int msgSize = sizeInfo.msgSize;

        int capacity = logSize - 4 - 4 - msgSize;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.clear();
        readFileChannel.read(buffer, phyOffset + 4 + 4 + msgSize);
        buffer.flip();
        int queueId = buffer.getInt();
        long queueOffset = buffer.getLong();
        byte[] topicBytes = new byte[capacity - 4 - 8];
        buffer.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.ISO_8859_1);
        long nextPhyOffset = phyOffset + logSize;
        return new TopicQueueOffsetInfo(topic, queueId, queueOffset, nextPhyOffset);
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
    private SizeInfo readSize(long physicalOffset) throws IOException {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4 + 4);
        sizeBuffer.clear();
        readFileChannel.read(sizeBuffer, physicalOffset);
        sizeBuffer.flip();
        return new SizeInfo(sizeBuffer.getInt(), sizeBuffer.getInt());
    }

    private int readMsgSize(long physicalOffset) throws IOException {
        ByteBuffer msgSizeBuffer = ByteBuffer.allocate(4);
        msgSizeBuffer.clear();
        readFileChannel.read(msgSizeBuffer, physicalOffset + 4);
        msgSizeBuffer.flip();
        return msgSizeBuffer.getInt();
    }

    static class SizeInfo {
        int logSize;
        int msgSize;

        public SizeInfo(int logSize, int msgSize) {
            this.logSize = logSize;
            this.msgSize = msgSize;
        }
    }
}
