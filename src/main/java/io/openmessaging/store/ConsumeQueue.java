package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.common.StopWare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    public static final int ITEM_SIZE = 8 + 8;

    private final Store store;

    private final ByteBuffer prefixSizeBuffer;

    private final ByteBuffer suffixBuffer;

    private final byte[] suffixBytes;

    public ConsumeQueue(Store store) {
        this.store = store;
        this.prefixSizeBuffer = ByteBuffer.allocateDirect(4 + 4);
        this.suffixBuffer = ByteBuffer.allocateDirect(120);
        this.suffixBytes = new byte[100];
    }

    // sync invoke
    public void syncFromCommitLog() throws IOException {
        long phyOffset = store.getCheckpoint().getPhyOffset();
        long commitLogWrotePosition = store.getCommitLog().readWrotePosition();
        log.debug("syncFromCommitLog starting, phyOffset: {}, commitLogWrotePosition: {}", phyOffset, commitLogWrotePosition);
        while (phyOffset < commitLogWrotePosition) {
            CommitLog.TopicQueueOffsetInfo info = store.getCommitLog().getOffset(phyOffset,
                    prefixSizeBuffer, suffixBuffer, suffixBytes);

            // write into consumeQueue
            store.getConsumeQueue().write(info.getTopic(), info.getQueueId(), info.getQueueOffset(), phyOffset);
            log.debug("syncFromCommitLog, wrote: {}", info);

            // update checkpoint
            store.getCheckpoint().updatePhyOffset(info.getNextPhyOffset());

            phyOffset = info.getNextPhyOffset();
        }
    }

    private final ByteBuffer wroteBuffer = ByteBuffer.allocateDirect(ITEM_SIZE);

    private final Set<String> topics = new HashSet<>();

    // (topic, queueId) -> fileChannel
    private final Map<String, FileChannel> fileChannelMap = new HashMap<>();

    //polish
    public void write(String topic, int queueId, long queueOffset, long commitLogOffset) throws IOException {
        // sync write file in (topic + queueId)

        if (!topics.contains(topic)) {
            Path dir = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                topics.add(topic);
            }
        }

        // get file path by topic and queueId
        FileChannel fileChannel = fileChannelMap.computeIfAbsent(topic + "|_%_|" + queueId, k -> {
            Path path = Paths.get(Config.getInstance().getConsumerQueueRootDir(), topic, String.valueOf(queueId));
            try {
                return FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("failed to open fileChannel", e);
                return null;
            }
        });
        if (fileChannel == null) {
            throw new IllegalStateException("failed to create file-channel");
        }

        // write
        wroteBuffer.clear();
        wroteBuffer.putLong(queueOffset);
        wroteBuffer.putLong(commitLogOffset);
        wroteBuffer.flip();
        fileChannel.write(wroteBuffer);
        fileChannel.force(true);
    }

    //----------------------------------------------------

    private final ByteBuffer loadMemBuffer = ByteBuffer.allocateDirect(ITEM_SIZE);

    public TopicQueueTable loadTopicQueueTable() throws IOException {
        TopicQueueTable table = new TopicQueueTable();

        File dir = new File(Config.getInstance().getConsumerQueueRootDir());
        File[] topicDirs = dir.listFiles();
        if (topicDirs == null || topicDirs.length == 0) {
            return table;
        }
        for (File topicDir : topicDirs) {
            String topicName = topicDir.getName();
            if (!topicDir.isDirectory()) {
                continue;
            }
            File[] queueFiles = topicDir.listFiles();
            if (queueFiles == null || queueFiles.length == 0) {
                continue;
            }
            for (File queueFile : queueFiles) {
                int queueId = Integer.parseInt(queueFile.getName());
                FileChannel fileChannel = FileChannel.open(queueFile.toPath(), StandardOpenOption.READ);

                ByteBuffer byteBuffer = loadMemBuffer;

                for (int position = 0; ; ) {
                    byteBuffer.clear();
                    int readBytes = fileChannel.read(byteBuffer, position);
                    if (readBytes <= 0) {
                        break;
                    }
                    byteBuffer.flip();
                    long queueOffset = byteBuffer.getLong();
                    long phyOffset = byteBuffer.getLong();
                    table.put(topicName, queueId, queueOffset, phyOffset);
                    log.debug("consumeQueue loadTopicQueueTable, put: ({}, {} -> {}, {})",
                            topicName, queueId, queueOffset, phyOffset);
                    position += readBytes;
                }
            }
        }
        return table;
    }

    @Override
    public void stop() {
        log.info("stopped");
    }
}
