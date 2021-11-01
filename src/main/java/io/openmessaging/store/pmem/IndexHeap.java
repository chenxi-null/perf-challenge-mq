package io.openmessaging.store.pmem;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;
import io.openmessaging.store.TopicQueueTable;
import io.openmessaging.util.FileUtil;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenxi20
 * @date 2021/10/29
 */
public class IndexHeap {

    private static final Logger log = LoggerFactory.getLogger(IndexHeap.class);

    private Config config = Config.getInstance();

    private Map<String, MemoryBlock> blocks = new HashMap<>();

    public void start() {
    }

    MemoryBlock findBlock(String topic, int queueId) throws IOException {
        String key = Util.buildKey(topic, queueId);
        MemoryBlock block = blocks.get(key);
        if (block != null) {
            return block;
        }

        MemoryBlock memoryBlock = createMemoryBlock(topic, queueId);
        blocks.put(key, memoryBlock);
        return memoryBlock;
    }

    private MemoryBlock createMemoryBlock(String topic, int queueId) throws IOException {
        log.debug("creating memory block");
        String dir = config.getPmemDir() + "/" + topic;
        FileUtil.createDirIfNotExists(dir);
        String filename = dir + "/" + queueId;

        long heapSize = config.getPmemIndexHeapSize();
        long blockSize = config.getPmemIndexMemoryBlockSize();
        Heap heap = Heap.exists(filename) ? Heap.openHeap(filename) : Heap.createHeap(filename, heapSize);
        MemoryBlock memoryBlock = heap.allocateMemoryBlock(blockSize, true);
        heap.setRoot(memoryBlock.handle());

        memoryBlock.setLong(0,8 + 8);
        log.info("created memory block");
        return memoryBlock;
    }

    // Data Structure:
    //  indexHeap: [dataBlock1, dataBlock2, ...]
    //      dataBlock: [currBlockWrotePosition, nextBlockHandle, indexItem1, indexItem2, ...]
    //      indexItem: (queueOffset, msgBlockHandle)
    //
    public void write(String topic, int queueId, long queueOffset, long msgBlockHandle) throws IOException {
        MemoryBlock block = findBlock(topic, queueId);

        long currBlockWrotePosition = block.getLong(0);
        log.trace("currBlockWrotePosition: {}", currBlockWrotePosition);
        block.setLong(currBlockWrotePosition, queueOffset);
        block.setLong(currBlockWrotePosition + 8, msgBlockHandle);
        block.setLong(0, currBlockWrotePosition + 16);
        block.flush();
    }

    public void load(TopicQueueTable topicQueueTable) throws IOException {
        log.debug("loading topicQueueTable");
        String dirPath = config.getPmemDir();
        File dir = new File(dirPath);
        File[] topicDirs = dir.listFiles();
        if (topicDirs == null || topicDirs.length == 0) {
            return;
        }
        log.trace("topicDirs: " + Arrays.toString(topicDirs));
        for (File topicDir : topicDirs) {
            if (topicDir.isFile()) {
                continue;
            }
            String topicName = topicDir.getName();
            File[] queueFiles = topicDir.listFiles();
            if (queueFiles == null || queueFiles.length == 0) {
                return;
            }
            log.trace("queueFiles: " + Arrays.toString(queueFiles));
            for (File queueFile : queueFiles) {
                String str = queueFile.getName();
                int queueId;
                try {
                    queueId = Integer.parseInt(str);
                } catch (NumberFormatException e) {
                    continue;
                }

                String filename = queueFile.getAbsolutePath();
                if (Heap.exists(filename)) {
                    Heap heap = Heap.openHeap(filename);
                    MemoryBlock block = heap.memoryBlockFromHandle(heap.getRoot());

                    // init blocks
                    this.blocks.put(Util.buildKey(topicName, queueId), block);

                    // load into table
                    load(topicQueueTable, topicName, queueId);
                }
            }
        }
    }

    // load in memory table
    public void load(TopicQueueTable topicQueueTable, String topic, int queueId) throws IOException {
        log.info("loading topicQueueTable, ({}, {})", topic, queueId);
        MemoryBlock block = findBlock(topic, queueId);

        long currBlockWrotePosition = block.getLong(0);
        for (long pos = 8 + 8; pos < currBlockWrotePosition; pos += 16) {
            long queueOffset = block.getLong(pos);
            long msgBlockHandle = block.getLong(pos + 8);
            log.trace("topicQueueTable put ({}, {}), queueOffset: {}, msgBlockHandle: {}",
                    topic, queueId, queueOffset, msgBlockHandle);
            topicQueueTable.putByPmem(topic, queueId, queueOffset, msgBlockHandle);
        }
        // TODO: load next block
    }
}
