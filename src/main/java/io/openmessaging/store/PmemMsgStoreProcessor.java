package io.openmessaging.store;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;

import java.nio.ByteBuffer;

/**
 * @author chenxi20
 * @date 2021/10/28
 */
public class PmemMsgStoreProcessor implements MsgStoreProcessor {

    private final Store store;

    private Heap msgHeap;

    private Heap indexHeap;

    public PmemMsgStoreProcessor(Store store) {
        this.store = store;
        init();
    }

    public void init() {
        initMsgHeap();
        initIndexHeap();
    }

    private void initIndexHeap() {
        String path = Config.getInstance().getPmemIndexHeapPath();
        long heapSize = Config.getInstance().getPmemIndexHeapSize();
        this.indexHeap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
    }

    private void initMsgHeap() {
        String path = Config.getInstance().getPmemMsgHeapPath();
        long heapSize = Config.getInstance().getPmemMsgHeapSize();
        this.msgHeap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
    }

    /*
    'msg':
    msgHeap: one msg one block

    'index':
    one queue one heap
        indexHeap: only one block - indexItems
            indexItem: (queueOffset, msgBlockHandle)

    ---

    write into msgHeap, create a new mem block, get block handle value
    write the handle value into indexHeap
     */
    @Override
    public long write(String topic, int queueId, ByteBuffer data) throws Exception {

        long msgBlockHandleValue = writeMsgHeap(data);

        return writeIndexHeap(topic, queueId, msgBlockHandleValue);
    }


    // Data Structure:
    //                    headBlock               dataBlock
    // indexHeap: [(wrotePosition, handle), (indexItem1, indexItem2, ...)]
    //                                          indexItem: (queueOffset, msgBlockHandle)
    private long writeIndexHeap(String topic, int queueId, long msgBlockHandleValue) {
        // calculate
        long queueOffset = 0;

        // find the index heap by topic+queueId

        // read from indexHeadBlock: get wrote position of index block
        long rootAddr = indexHeap.getRoot();
        MemoryBlock indexHeadBlock = indexHeap.memoryBlockFromHandle(rootAddr);
        long wrotePosition = indexHeadBlock.getLong(0);
        long indexBlockHandle = indexHeadBlock.getLong(8);

        // write into indexDataBlock: append indexItem to index block
        MemoryBlock indexDataBlock = indexHeap.memoryBlockFromHandle(indexBlockHandle);
        indexDataBlock.setLong(wrotePosition, queueOffset);
        indexDataBlock.setLong(wrotePosition + 8, msgBlockHandleValue);
        indexDataBlock.flush();

        return queueOffset;
    }

    /**
     * @return msgBlockHandleValue
     */
    private long writeMsgHeap(ByteBuffer data) {
        int msgSize = data.remaining();
        MemoryBlock msgBlock = msgHeap.allocateMemoryBlock(msgSize, true);
        byte[] msgBytes = byteBufferToByteArray(data);
        msgBlock.copyFromArray(msgBytes, 0, 0, msgSize);
        msgBlock.flush();
        return msgBlock.handle();
    }

    private byte[] byteBufferToByteArray(ByteBuffer data) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return bytes;
    }

    @Override
    public ByteBuffer getData(String topic, int queueId, long offset) throws Exception {
        return null;
    }
}
