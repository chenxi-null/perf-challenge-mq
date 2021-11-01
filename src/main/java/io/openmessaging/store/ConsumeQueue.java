package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.common.StopWare;
import io.openmessaging.util.ChecksumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * data struct
 * <p>
 * item is length-fixed:
 * - queueOffset
 * - commitLogOffset
 */
public class ConsumeQueue implements StopWare {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueue.class);

    public static final int ITEM_SIZE = 8 + 8 + 4;

    private final Store store;

    private final ByteBuffer logItemHeadSizeBuffer;

    private final ByteBuffer logItemTailBuffer;

    private final ByteBuffer itemBuffer = ByteBuffer.allocateDirect(ITEM_SIZE);

    private final Set<String> topicDirs = new HashSet<>();

    // (topic, queueId) -> fileChannel
    private final Map<String, FileChannel> fileChannelMap = new HashMap<>();

    private long processedPhysicalOffset;

    private TopicQueueTable topicQueueTable;

    public ConsumeQueue(Store store) {
        this.store = store;
        this.logItemHeadSizeBuffer = ByteBuffer.allocateDirect(4 + 4);
        // buffer length: max topic length + 16
        this.logItemTailBuffer = ByteBuffer.allocateDirect(120);
    }

    // sync invoke
    public void syncFromCommitLog() throws IOException {
        long processedPhyOffset = getProcessedPhysicalOffset();
        long commitLogWrotePosition = store.getCommitLog().getWrotePosition();
        log.debug("syncFromCommitLog starting, processedPhyOffset: {}, commitLogWrotePosition: {}",
                processedPhyOffset, commitLogWrotePosition);
        while (processedPhyOffset < commitLogWrotePosition) {
            CommitLog.LogicItemInfo info = store.getCommitLog().getLogicItemInfo(processedPhyOffset,
                    logItemHeadSizeBuffer, logItemTailBuffer);

            // write into consumeQueue
            write(info.getTopic(), info.getQueueId(), info.getQueueOffset(), processedPhyOffset);
            log.debug("syncFromCommitLog, wrote: {}", info);

            processedPhyOffset = info.getNextPhyOffset();
        }
        if (processedPhyOffset > commitLogWrotePosition) {
            log.error("[bug] processedPhyOffset > commitLogWrotePosition");
        }
        setProcessedPhysicalOffset(commitLogWrotePosition);
    }

    private void write(String topic, int queueId, long queueOffset, long physicalOffset) throws IOException {

        ensureTopicDirExist(topic);

        FileChannel fileChannel = getFileChannel(topic, queueId);

        itemBuffer.clear();
        itemBuffer.putLong(queueOffset);
        itemBuffer.putLong(physicalOffset);
        itemBuffer.putInt(ChecksumUtil.get(queueOffset, physicalOffset));
        itemBuffer.flip();
        fileChannel.write(itemBuffer);
        fileChannel.force(true);
    }

    // only for testing
    TopicQueueTable loadTopicQueueTable() throws IOException {
        return doRecover();
    }

    /**
     * - check data item
     * - update processedPhysicalOffset
     * - update mem topicQueueTable
     *
     * @return processedPhysicalOffset
     */
    public long recover() throws IOException {
        this.topicQueueTable = doRecover();
        return getProcessedPhysicalOffset();
    }

    /**
     * - check data item
     * - update processedPhysicalOffset
     * - update mem topicQueueTable
     */
    private TopicQueueTable doRecover() throws IOException {
        // TODO: move TopicQueueTable to store
        TopicQueueTable table = new TopicQueueTable();

        File dir = new File(Config.getInstance().getConsumerQueueRootDir());
        File[] topicDirs = dir.listFiles();
        if (topicDirs == null || topicDirs.length == 0) {
            return table;
        }
        for (File topicDir : topicDirs) {
            if (!topicDir.isDirectory()) {
                continue;
            }
            File[] queueFiles = topicDir.listFiles();
            if (queueFiles == null || queueFiles.length == 0) {
                continue;
            }
            String topicName = topicDir.getName();
            for (File queueFile : queueFiles) {
                int queueId = Integer.parseInt(queueFile.getName());
                checkDataAndUpdateTopicQueueTable(table, topicName, queueId);
            }
        }
        return table;
    }

    private void checkDataAndUpdateTopicQueueTable(
            TopicQueueTable table, String topicName, int queueId) throws IOException {
        log.debug("checkDataAndUpdateTopicQueueTable, topic: {}, queueId: {}", topicName, queueId);

        FileChannel fileChannel = getFileChannel(topicName, queueId);
        ByteBuffer byteBuffer = itemBuffer;
        for (int position = 0; ; ) {
            byteBuffer.clear();
            int readBytes = fileChannel.read(byteBuffer, position);
            if (readBytes <= 0) {
                break;
            }

            byteBuffer.flip();
            if (byteBuffer.remaining() < 8) {
                break;
            }
            long queueOffset = byteBuffer.getLong();
            if (byteBuffer.remaining() < 8) {
                break;
            }
            long phyOffset = byteBuffer.getLong();
            if (byteBuffer.remaining() < 4) {
                break;
            }
            int checkSum = byteBuffer.getInt();
            if (!ChecksumUtil.check(queueOffset, phyOffset, checkSum)) {
                log.warn("occur item tail dirty data");
               break;
            }

            setProcessedPhysicalOffset(phyOffset);

            table.put(topicName, queueId, queueOffset, phyOffset);
            log.debug("topicQueueTable, put: ({}, {} -> {}, {})",
                    topicName, queueId, queueOffset, phyOffset);

            position += readBytes;
        }
    }

    private void ensureTopicDirExist(String topic) throws IOException {
        if (!topicDirs.contains(topic)) {
            Path dir = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                topicDirs.add(topic);
            }
        }
    }

    private FileChannel getFileChannel(String topic, int queueId) {
        FileChannel fileChannel = fileChannelMap.computeIfAbsent(buildKey(topic, queueId), k -> {
            File file = new File(Config.getInstance().getConsumerQueueRootDir() + "/" + topic + "/" + queueId);
            try {
                return new RandomAccessFile(file, "rw").getChannel();
            } catch (IOException e) {
                log.error("failed to get fileChannel", e);
                return null;
            }
        });
        if (fileChannel == null) {
            throw new IllegalStateException("failed to create file-channel");
        }
        return fileChannel;
    }

    private String buildKey(String topic, int queueId) {
        return topic + "|_%_|" + queueId;
    }

    @Override
    public void stop() {
        log.info("stopped");
    }

    private long getProcessedPhysicalOffset() {
        return processedPhysicalOffset;
    }

    private void setProcessedPhysicalOffset(long processedPhysicalOffset) {
        this.processedPhysicalOffset = processedPhysicalOffset;
    }

    public TopicQueueTable getTopicQueueTable() {
        return this.topicQueueTable;
    }
}
