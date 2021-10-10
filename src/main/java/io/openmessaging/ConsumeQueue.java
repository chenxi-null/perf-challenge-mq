package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * data struct
 * <p>
 * item is length-fixed:
 * - queueOffset
 * - commitLogOffset
 */
public class ConsumeQueue {

    public static final int ITEM_SIZE = 4 + 8;

    private final Store store;

    public ConsumeQueue(Store store) {
        this.store = store;
    }

    /**
     * @return queueOffset
     */
    public long write(String topic, int queueId, long commitLogOffset) throws IOException {
        // sync write file in (topic + queueId)

        Path dir = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        // get file path by topic and queueId
        Path path = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic, String.valueOf(queueId));
        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);

        // calculate queueOffset
        long queueOffset = Files.size(path) / ITEM_SIZE;

        // write
        ByteBuffer byteBuffer = ByteBuffer.allocate(ITEM_SIZE);
        byteBuffer.putInt((int) queueOffset);
        byteBuffer.putLong(commitLogOffset);
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.force(true);
        return queueOffset;
    }

    public long findCommitLogOffset(String topic, int queueId, long queueOffset) throws IOException {
        Path path = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic, String.valueOf(queueId));
        if (!Files.exists(path)) {
            return -1;
        }
        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);

        long position = ITEM_SIZE * queueOffset;

        ByteBuffer byteBuffer = ByteBuffer.allocate(ITEM_SIZE);
        byteBuffer.clear();
        int read = fileChannel.read(byteBuffer, position);
        if (read <= 0) {
            return -1;
        }
        return byteBuffer.getLong(4);
    }
}
