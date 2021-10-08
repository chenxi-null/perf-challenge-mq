package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue {

    private final StoreMgr storeMgr;

    public DefaultMessageQueueImpl() {
        this.storeMgr = new StoreMgr();
        try {
            init();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() throws IOException {
        if (!Files.exists(Config.getInstance().getCommitLogPath())) {
            Files.createFile(Config.getInstance().getCommitLogPath());
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        try {
            return storeMgr.write(topic, queueId, data);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long startOffset, int fetchNum) {
        Map<Integer, ByteBuffer> map = new HashMap<>();
        for (int i = 0; i < fetchNum; i++) {
            long offset = startOffset + i;
            ByteBuffer data;
            try {
                data = storeMgr.getData(topic, queueId, offset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (data != null) {
                map.put(i, data);
            }
        }
        return map;
    }

    static class StoreMgr {

        private final CommitLog commitLog;

        private final ConsumerQueue consumerQueue;

        public StoreMgr() {
            this.consumerQueue = new ConsumerQueue();
            this.commitLog = new CommitLog();
        }

        public long write(String topic, int queueId, ByteBuffer data) throws IOException {
            // sync write into commitLog
            long commitLogOffset = commitLog.write(topic, queueId, data);

            // write into consumerQueue
            return consumerQueue.write(topic, queueId, commitLogOffset);
        }

        public ByteBuffer getData(String topic, int queueId, long offset) throws IOException {
            long commitLogOffset = consumerQueue.findCommitLogOffset(topic, queueId, offset);
            if (commitLogOffset < 0) {
                return null;
            }
            return commitLog.getData(commitLogOffset);
        }
    }

    /**
     * data struct
     * <p>
     * - msgSize
     * - body
     * - queueId
     * - queueOffset
     * - topic
     */
    static class CommitLog {

        /**
         * @return commitLogOffset
         */
        public long write(String topic, int queueId, ByteBuffer data) throws IOException {
            FileChannel fileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);

            long commitLogOffset = Files.size(Config.getInstance().getCommitLogPath());

            byte[] topicBytes = topic.getBytes(StandardCharsets.ISO_8859_1);

            int msgSize = data.capacity();
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + msgSize + 4 + topicBytes.length);
            byteBuffer.putInt(msgSize);
            byteBuffer.put(data);
            byteBuffer.putInt(queueId);
            //byteBuffer.put()
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

    /**
     * data struct
     * <p>
     * item is length-fixed:
     * - queueOffset
     * - commitLogOffset
     */
    static class ConsumerQueue {

        public static final int ITEM_SIZE = 4 + 8;

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
}
