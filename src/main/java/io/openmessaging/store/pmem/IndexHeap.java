package io.openmessaging.store.pmem;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;
import io.openmessaging.store.TopicQueueTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenxi20
 * @date 2021/10/29
 */
public class IndexHeap {

    private static final Logger log = LoggerFactory.getLogger(IndexHeap.class);

    private Heap heap;

    public void start() {
        String path = Config.getInstance().getPmemIndexHeapPath();
        long heapSize = Config.getInstance().getPmemIndexHeapSize();
        this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        MemoryBlock memoryBlock = heap.allocateMemoryBlock(Config.getInstance().getPmemIndexMemoryBlockSize(), true);
        heap.setRoot(memoryBlock.handle());

        memoryBlock.setLong(0,8 + 8);
    }

    // Data Structure:
    //  indexHeap: [dataBlock1, dataBlock2, ...]
    //      dataBlock: [currBlockWrotePosition, nextBlockHandle, indexItem1, indexItem2, ...]
    //      indexItem: (queueOffset, msgBlockHandle)
    //
    public void write(String topic, int queueId, long queueOffset, long msgBlockHandle) {

        // find the index heap by topic+queueId
        //  create heap and block if needed
        MemoryBlock block = heap.memoryBlockFromHandle(heap.getRoot());

        long currBlockWrotePosition = block.getLong(0);
        log.trace("currBlockWrotePosition: {}", currBlockWrotePosition);
        block.setLong(currBlockWrotePosition, queueOffset);
        block.setLong(currBlockWrotePosition + 8, msgBlockHandle);
        block.setLong(0, currBlockWrotePosition + 16);
        block.flush();
    }

    // load in mem
    public void load(TopicQueueTable topicQueueTable, String topic, int queueId) {
        // find the index heap by topic+queueId
        MemoryBlock block = heap.memoryBlockFromHandle(heap.getRoot());

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
