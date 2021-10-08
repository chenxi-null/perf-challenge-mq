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
    }

    public long _append(String topic, int queueId, ByteBuffer data) {
        try {
            return storeMgr.write(topic, queueId, data);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    public Map<Integer, ByteBuffer> _getRange(String topic, int queueId, long startOffset, int fetchNum) {
        Map<Integer, ByteBuffer> map = new HashMap<>();
        for (int i = 0; i < fetchNum; i++) {
            long offset = startOffset + i;
            ByteBuffer data = storeMgr.getData(topic, queueId, offset);
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

            // async write into consumerQueue
            return consumerQueue.write(topic, queueId, commitLogOffset);
        }

        public ByteBuffer getData(String topic, int queueId, long offset) {
            long commitLogOffset = consumerQueue.findCommitLogOffset(topic, queueId, offset);
            return commitLog.getData(commitLogOffset);
        }
    }

    static class CommitLog {
        public long write(String topic, int queueId, ByteBuffer data) {
            return 0;
        }

        public ByteBuffer getData(long commitLogOffset) {
            // file partition
            return null;
        }
    }

    /**
     * data struct
     *
     * item is length-fixed:
     * - queueOffset
     * - commitLogOffset
     */
    static class ConsumerQueue {

        public static final String ROOT_DIR = "/essd";

        public static final int ITEM_SIZE = 4 + 8;

        /**
         * @return queueOffset
         */
        public long write(String topic, int queueId, long commitLogOffset) throws IOException {
            // sync write file in (topic + queueId)

            // get file path by topic and queueId
            Path path = Paths.get(ROOT_DIR, topic, String.valueOf(queueId));
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

        public long findCommitLogOffset(String topic, int queueId, long queueOffset) {
            return 0;
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

    //----------------------------------------------------

    ConcurrentHashMap<String, Map<Integer, Long>> appendOffset = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();

    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue) {
        V retObj = map.get(key);
        if (retObj != null) {
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        // 获取该 topic-queueId 下的最大位点 offset
        Map<Integer, Long> topicOffset = getOrPutDefault(appendOffset, topic, new HashMap<>());
        long offset = topicOffset.getOrDefault(queueId, 0L);
        // 更新最大位点
        topicOffset.put(queueId, offset + 1);

        Map<Integer, Map<Long, ByteBuffer>> map1 = getOrPutDefault(appendData, topic, new HashMap<>());
        Map<Long, ByteBuffer> map2 = getOrPutDefault(map1, queueId, new HashMap<>());
        // 保存 data 中的数据
        ByteBuffer buf = ByteBuffer.allocate(data.remaining());
        buf.put(data);
        map2.put(offset, buf);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        for (int i = 0; i < fetchNum; i++) {
            Map<Integer, Map<Long, ByteBuffer>> map1 = appendData.get(topic);
            if (map1 == null) {
                break;
            }
            Map<Long, ByteBuffer> m2 = map1.get(queueId);
            if (m2 == null) {
                break;
            }
            ByteBuffer buf = m2.get(offset + i);
            if (buf != null) {
                // 返回前确保 ByteBuffer 的 remain 区域为完整答案
                buf.position(0);
                buf.limit(buf.capacity());
                ret.put(i, buf);
            }
        }
        return ret;
    }
}
