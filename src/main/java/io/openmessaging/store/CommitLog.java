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
        long commitLogOffset = Files.size(Config.getInstance().getCommitLogPath());

        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.capacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + msgSize + 4 + 8 + topicBytes.length);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putLong(queueOffset);
        byteBuffer.put(topicBytes);

        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.force(true);

        updateTopicQueueTable(topic, queueId, queueOffset, commitLogOffset);
    }

    private void updateTopicQueueTable(String topic, int queueId, long queueOffset, long commitLogOffset) {
        store.getTopicQueueTable().put(topic, queueId, queueOffset, commitLogOffset);
    }

    public ByteBuffer getData(long commitLogOffset) throws IOException {
        ByteBuffer msgSizeBuffer = ByteBuffer.allocate(4);
        msgSizeBuffer.clear();
        fileChannel.read(msgSizeBuffer, commitLogOffset);
        msgSizeBuffer.flip();
        int msgSize = msgSizeBuffer.getInt();

        ByteBuffer buffer = ByteBuffer.allocate(msgSize);
        buffer.clear();
        fileChannel.read(buffer, commitLogOffset + 4);
        return buffer;
    }
}
