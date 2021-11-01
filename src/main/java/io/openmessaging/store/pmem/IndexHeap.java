package io.openmessaging.store.pmem;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;
import io.openmessaging.store.Store;
import io.openmessaging.store.TopicQueueTable;
import io.openmessaging.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.openmessaging.util.Util.buildKey;

/**
 * @author chenxi20
 * @date 2021/10/29
 */
public class IndexHeap {

    // Data Structure:
    //  indexHeap: [dataBlock1, dataBlock2, ...]
    //      dataBlock: [currBlockWrotePosition, nextBlockHandle, indexItem1, indexItem2, ...]
    //      indexItem: (queueOffset, msgBlockHandle)
    //

    private static final Logger log = LoggerFactory.getLogger(IndexHeap.class);

    private Config config = Config.getInstance();

    private Map<String, MemoryBlock> latestBlocks = new HashMap<>();

    private Map<String, Heap> heaps = new HashMap<>();

    private Store store;

    public IndexHeap(Store store) {
        this.store = store;
    }

    public void start() {
    }

    MemoryBlock findFirstBlock(String topic, int queueId) throws IOException {
        Heap heap = getHeap(topic, queueId);
        return heap.memoryBlockFromHandle(heap.getRoot());
    }

    MemoryBlock findLatestBlock(String topic, int queueId) throws IOException {
        String key = buildKey(topic, queueId);
        MemoryBlock block = latestBlocks.get(key);
        if (block != null) {
            return block;
        }

        MemoryBlock memoryBlock = createMemoryBlock(topic, queueId);
        latestBlocks.put(key, memoryBlock);
        return memoryBlock;
    }

    private MemoryBlock createMemoryBlock(String topic, int queueId) throws IOException {
        log.debug("creating memory block");

        long blockSize = config.getPmemIndexMemoryBlockSize();
        Heap heap = createHeap(topic, queueId);

        MemoryBlock memoryBlock = heap.allocateMemoryBlock(blockSize, true);
        heap.setRoot(memoryBlock.handle());

        initBlockWrotePosition(memoryBlock);
        log.info("created memory block");
        return memoryBlock;
    }

    private Heap getHeap(String topic, int queueId) throws IOException {
        Heap heap = heaps.get(buildKey(topic, queueId));
        if (heap != null) {
            return heap;
        }
        Heap h = createHeap(topic, queueId);
        heaps.put(buildKey(topic, queueId), h);
        return h;
    }

    private Heap createHeap(String topic, int queueId) throws IOException {
        String dir = config.getPmemDir() + "/" + topic;
        FileUtil.createDirIfNotExists(dir);
        String filename = dir + "/" + queueId;
        return Heap.exists(filename) ?
                Heap.openHeap(filename) : Heap.createHeap(filename, config.getPmemIndexHeapSize());
    }

    public void write(String topic, int queueId, long queueOffset, long msgBlockHandle) throws IOException {
        long blockSize = config.getPmemIndexMemoryBlockSize();
        MemoryBlock block = findLatestBlock(topic, queueId);

        long currBlockWrotePosition = block.getLong(0);
        log.trace("currBlockWrotePosition: {}", currBlockWrotePosition);

        if (currBlockWrotePosition < blockSize) {
            appendIndexItemToBlock(queueOffset, msgBlockHandle, block, currBlockWrotePosition);
        } else {
            log.debug("create new block");
            MemoryBlock newBlock = getHeap(topic, queueId).allocateMemoryBlock(blockSize, true);
            long wrotePosition = initBlockWrotePosition(newBlock);

            updateBlockNextHandle(block, newBlock.handle());

            appendIndexItemToBlock(queueOffset, msgBlockHandle, newBlock, wrotePosition);

            latestBlocks.put(buildKey(topic, queueId), newBlock);
        }
    }

    private void appendIndexItemToBlock(long queueOffset, long msgBlockHandle, MemoryBlock block, long blockWrotePosition) {
        block.setLong(blockWrotePosition, queueOffset);
        block.setLong(blockWrotePosition + 8, msgBlockHandle);
        block.setLong(0, blockWrotePosition + 16);
        block.flush();
    }

    private void updateBlockNextHandle(MemoryBlock memoryBlock, long nextBlockHandle) {
        memoryBlock.setLong(8, nextBlockHandle);
    }

    private int initBlockWrotePosition(MemoryBlock memoryBlock) {
        int wrotePosition = 8 + 8;
        memoryBlock.setLong(0, wrotePosition);
        return wrotePosition;
    }

    // only for testing
    public TopicQueueTable load() throws IOException {
        TopicQueueTable t = new TopicQueueTable();
        load(t);
        return t;
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
                    this.latestBlocks.put(buildKey(topicName, queueId), block);

                    // load into table
                    load(topicQueueTable, topicName, queueId);
                }
            }
        }
    }

    // load in memory table
    public void load(TopicQueueTable topicQueueTable, String topic, int queueId) throws IOException {
        log.info("loading topicQueueTable, ({}, {})", topic, queueId);
        MemoryBlock block = findFirstBlock(topic, queueId);
        while (true) {
            long blockWrotePosition = block.getLong(0);
            for (long pos = 8 + 8; pos < blockWrotePosition; pos += 16) {
                long queueOffset = block.getLong(pos);
                long msgBlockHandle = block.getLong(pos + 8);
                updateMemTable(topicQueueTable, topic, queueId, queueOffset, msgBlockHandle);
            }

            long nextBlockHandle = block.getLong(8);
            if (nextBlockHandle <= 0) {
                break;
            }
            block = getHeap(topic, queueId).memoryBlockFromHandle(nextBlockHandle);
        }
    }

    void updateMemTable(TopicQueueTable topicQueueTable, String topic, int queueId, long queueOffset, long msgBlockHandle) {
        log.trace("topicQueueTable put ({}, {}), queueOffset: {}, msgBlockHandle: {}",
                topic, queueId, queueOffset, msgBlockHandle);
        topicQueueTable.putByPmem(topic, queueId, queueOffset, msgBlockHandle);
    }
}
