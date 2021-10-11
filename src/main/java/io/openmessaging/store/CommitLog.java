package io.openmessaging.store;

import io.openmessaging.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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

    public CommitLog(Store store) throws IOException {
        this.store = store;

        this.writeFileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        this.readFileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                StandardOpenOption.READ);
    }

    /**
     * @return commitLogOffset
     */
    public void write(String topic, int queueId, long queueOffset, ByteBuffer data) throws IOException {
        // read wrotePosition of commitLog
        long physicalOffset = readWrotePosition();
        //long physicalOffset = Files.size(Config.getInstance().getCommitLogPath());

        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.capacity();
        int bufferSize = 4 /* logSize */
                + 4 /* msgSize */ + msgSize
                + 4 /* queueId */
                + 8 /* queueOffset */
                + topicBytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.putInt(bufferSize);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(queueOffset);
        //byteBuffer.putLong(physicalOffset + bufferSize);
        byteBuffer.put(topicBytes);

        byteBuffer.flip();
        writeFileChannel.write(byteBuffer);
        writeFileChannel.force(true);

        // update wrotePosition of commitLog
        long nextPhysicalOffset = physicalOffset + bufferSize;
        updateWrotePosition(nextPhysicalOffset);
        log.info("commitLog wrote, physicalOffset: {}, nextPhysicalOffset: {}", physicalOffset, nextPhysicalOffset);

        updateTopicQueueTable(topic, queueId, queueOffset, physicalOffset);
    }

    private long tempMemWrotePosition = 0;

    public long readWrotePosition() {
        return tempMemWrotePosition;
    }

    public void updateWrotePosition(long nextPhysicalOffset) {
        tempMemWrotePosition = nextPhysicalOffset;
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
