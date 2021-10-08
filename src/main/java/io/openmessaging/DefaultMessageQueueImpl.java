package io.openmessaging;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
            map.put(i, data);
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

            fileChannel.write(byteBuffer);
            return commitLogOffset;
        }

        public ByteBuffer getData(long commitLogOffset) throws IOException {
            // file partition

            FileChannel fileChannel = FileChannel.open(Paths.get(Config.getInstance().getCommitLogFile()),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);

            ByteBuffer msgSizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(msgSizeBuffer, commitLogOffset);
            int msgSize = msgSizeBuffer.getInt();

            ByteBuffer buffer = ByteBuffer.allocate(msgSize);
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
            fileChannel.write(byteBuffer);
            fileChannel.force(true);
            return queueOffset;
        }

        public long findCommitLogOffset(String topic, int queueId, long queueOffset) throws IOException {
            Path path = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic, String.valueOf(queueId));
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);

            long position = ITEM_SIZE * queueOffset - 1;

            ByteBuffer byteBuffer = ByteBuffer.allocate(ITEM_SIZE);
            int read = fileChannel.read(byteBuffer, position);
            if (read <= 0) {
                return -1;
            }
            return byteBuffer.getLong(4);
        }
    }

    static class FileIO {

        public static final int NUM_BYTES = 8 * 1024;

        private final ConcurrentHashMap<String, Writer> filenameToOutput = new ConcurrentHashMap<>();

        public void write(Path path, String content) throws IOException {
            String filename = path.toString();
            Writer outputStream = filenameToOutput.computeIfAbsent(filename,
                    fn -> {
                        try {
                            OutputStreamImpl out = new OutputStreamImpl(
                                    FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                                            StandardOpenOption.CREATE));
                            return new BufferedWriter(
                                    new OutputStreamWriter(out, StandardCharsets.ISO_8859_1),
                                    NUM_BYTES);
                        } catch (IOException e) {
                            e.printStackTrace();
                            //Utils.log("failed to create `Writer`, file: %s, ex: %s", fn, e.getMessage());
                            return null;
                        }
                    });
            if (outputStream == null) {
                return;
            }
            outputStream.write(content);
        }

        public void flushAllOutputs() throws IOException {
            for (Writer outputStream : filenameToOutput.values()) {
                outputStream.flush();
            }
        }

        static class OutputStreamImpl extends OutputStream {

            private final FileChannel fileChannel;

            private final ByteBuffer byteBuffer;

            public OutputStreamImpl(FileChannel fileChannel) {
                this.fileChannel = fileChannel;
                this.byteBuffer = ByteBuffer.allocateDirect(NUM_BYTES * 2);
            }

            @Override
            public void write(int b) throws IOException {
                throw new UnsupportedEncodingException("should not be called");
            }

            public void write(byte[] bs, int off, int len) throws IOException {
                byteBuffer.clear();
                byteBuffer.put(bs, off, len);

                byteBuffer.flip();
                fileChannel.write(byteBuffer);
            }
        }
    }
}
