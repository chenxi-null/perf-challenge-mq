package io.openmessaging.store.pmem;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;
import io.openmessaging.store.TopicQueueTable;
import io.openmessaging.util.FileUtil;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        String dir = config.getPmemDir() + "/" + topic + "/" + queueId + "/index_heap";
        FileUtil.createDirIfNotExists(dir);

        long heapSize = config.getPmemIndexHeapSize();
        long blockSize = config.getPmemIndexMemoryBlockSize();
        Heap heap = Heap.exists(dir) ? Heap.openHeap(dir) : Heap.createHeap(dir, heapSize);
        MemoryBlock memoryBlock = heap.allocateMemoryBlock(blockSize, true);
        heap.setRoot(memoryBlock.handle());

        memoryBlock.setLong(0,8 + 8);
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

    // load in memory table
    public void load(TopicQueueTable topicQueueTable, String topic, int queueId) throws IOException {
        MemoryBlock block = findBlock(topic, queueId);

        long currBlockWrotePosition = block.getLong(0);
        for (long pos = 8 + 8; pos < currBlockWrotePosition; pos += 16) {
            long queueOffset = block.getLong(pos);
            long msgBlockHandle = block.getLong(pos + 8);
            log.trace("topicQueueTable put ({}, {}), queueOffset: {}, msgBlockHandle: {}",
                    topic, queueId, queueOffset, msgBlockHandle);
            topicQueueTable.putByPmem(topic, queueId, queueOffset, msgBlockHandle);
        }
    }
}
