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

    public ConsumeQueue(Store store) {
        this.store = store;
    }

    public void syncFromCommitLog() throws IOException {
        long phyOffset = store.getCheckpoint().getPhyOffset();
        long commitLogWrotePosition = store.getCommitLog().readWrotePosition();
        log.info("syncFromCommitLog starting, phyOffset: {}, commitLogWrotePosition: {}", phyOffset, commitLogWrotePosition);
        while (phyOffset < commitLogWrotePosition) {
            CommitLog.TopicQueueOffsetInfo info = store.getCommitLog().getOffset(phyOffset);

            // write into consumeQueue
            store.getConsumeQueue().write(info.getTopic(), info.getQueueId(), info.getQueueOffset(), phyOffset);
            log.info("syncFromCommitLog, wrote: {}", info);

            // update checkpoint
            store.getCheckpoint().updatePhyOffset(info.getNextPhyOffset());

            phyOffset = info.getNextPhyOffset();
        }
    }

    //polish
    public void write(String topic, int queueId, long queueOffset, long commitLogOffset) throws IOException {
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
        //long queueOffset = Files.size(path) / ITEM_SIZE;

        // write
        ByteBuffer byteBuffer = ByteBuffer.allocate(ITEM_SIZE);
        byteBuffer.putLong(queueOffset);
        byteBuffer.putLong(commitLogOffset);
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        fileChannel.force(true);
    }

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

                ByteBuffer byteBuffer = ByteBuffer.allocate(ITEM_SIZE);


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
                    log.info("consumeQueue loadTopicQueueTable, put: ({}, {} -> {}, {})",
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
