package io.openmessaging;

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

    public CommitLog(Store store) {
        this.store = store;
    }

    /**
     * @return commitLogOffset
     */
    public long write(String topic, int queueId, int queueOffset, ByteBuffer data) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        long commitLogOffset = Files.size(Config.getInstance().getCommitLogPath());

        byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

        int msgSize = data.capacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + msgSize + 4 + 4 + topicBytes.length);
        byteBuffer.putInt(msgSize);
        byteBuffer.put(data);
        byteBuffer.putInt(queueId);
        byteBuffer.putInt(queueOffset);
        byteBuffer.put(topicBytes);

        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.force(true);
        return commitLogOffset;
    }

    public ByteBuffer getData(long commitLogOffset) throws IOException {
        // file partition

        FileChannel fileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                StandardOpenOption.READ);

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
