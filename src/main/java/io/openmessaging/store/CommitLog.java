package io.openmessaging.store;

import io.openmessaging.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

    private final Store store;

    private final FileChannel fileChannel;

    public CommitLog(Store store) throws IOException {
        this.store = store;
        this.fileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    /**
     * @return commitLogOffset
     */
    public void write(String topic, int queueId, long queueOffset, ByteBuffer data) throws IOException {
        // read wrotePosition of commitLog
        long physicalOffset = Files.size(Config.getInstance().getCommitLogPath());

        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.capacity();
        int bufferSize =
                + 4 /* logSize */
                + 4 /* msgSize */ + msgSize
                + 4 /* queueId */
                + 8 /* queueOffset */
                + 8 /* physicalOffset */
                + topicBytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(queueOffset);
        //byteBuffer.putLong(physicalOffset + bufferSize);
        byteBuffer.put(topicBytes);

        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.force(true);

        // update wrotePosition of commitLog

        updateTopicQueueTable(topic, queueId, queueOffset, physicalOffset);
    }

    private void updateTopicQueueTable(String topic, int queueId, long queueOffset, long physicalOffset) {
        store.getTopicQueueTable().put(topic, queueId, queueOffset, physicalOffset);
    }

    public ByteBuffer getData(long physicalOffset) throws IOException {
        int msgSize = readMsgSize(physicalOffset);

        ByteBuffer buffer = ByteBuffer.allocate(msgSize);
        buffer.clear();
        fileChannel.read(buffer, physicalOffset + 4);
        return buffer;
    }

    // return: topic, queueId, queueOffset and nextPhyOffset
    public TopicQueueOffsetInfo getOffset(long phyOffset) throws IOException {
        int msgSize = readMsgSize(phyOffset);
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8);
        buffer.clear();
        fileChannel.read(buffer, phyOffset + 4 + msgSize + 4);
        long queueOffset = buffer.getLong();
        long nextPhyOffset = buffer.getLong();
        return new TopicQueueOffsetInfo(queueOffset, nextPhyOffset);
    }

    public static class TopicQueueOffsetInfo {
        long queueOffset;
        long nextPhyOffset;

        public TopicQueueOffsetInfo(long queueOffset, long nextPhyOffset) {
            this.queueOffset = queueOffset;
            this.nextPhyOffset = nextPhyOffset;
        }
    }

    private int readMsgSize(long physicalOffset) throws IOException {
        ByteBuffer msgSizeBuffer = ByteBuffer.allocate(4);
        msgSizeBuffer.clear();
        fileChannel.read(msgSizeBuffer, physicalOffset);
        msgSizeBuffer.flip();
        return msgSizeBuffer.getInt();
    }
    // get queueOffset and next phyOffset
}
