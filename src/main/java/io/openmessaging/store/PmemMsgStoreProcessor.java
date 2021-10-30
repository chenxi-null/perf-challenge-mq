package io.openmessaging.store;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.Config;
import io.openmessaging.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author chenxi20
 * @date 2021/10/28
 */
public class PmemMsgStoreProcessor implements MsgStoreProcessor {

    private static final Logger log = LoggerFactory.getLogger(PmemMsgStoreProcessor.class);

    private final Store store;

    private Heap msgHeap;

    private Heap indexHeap;

    public PmemMsgStoreProcessor(Store store) {
        this.store = store;
    }

    public void start() throws IOException {
        FileUtil.createDirIfNotExists(Config.getInstance().getPmemDir());

        initMsgHeap();
        //initIndexHeap();
    }

    private void initMsgHeap() {
        String path = Config.getInstance().getPmemMsgHeapPath();
        long heapSize = Config.getInstance().getPmemMsgHeapSize();
        this.msgHeap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
    }

    private void initIndexHeap() {
        String path = Config.getInstance().getPmemIndexHeapPath();
        long heapSize = Config.getInstance().getPmemIndexHeapSize();
        this.indexHeap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
    }

    // pmem storage design:
    //
    // 'msg':
    // msgHeap: one msg one block
    //
    // 'index':
    // one queue one heap
    //     indexHeap: only one block - indexItems
    //         indexItem: (queueOffset, msgBlockHandle)
    //
    //
    // write into msgHeap, create a new mem block, get block handle value
    // write the handle value into indexHeap
    @Override
    public long write(String topic, int queueId, ByteBuffer data) throws Exception {

        long queueOffset = topicQueueTable().calcNextQueueOffset(topic, queueId);

        long msgBlockHandle = writeMsgHeap(data);

        log.trace("writeIndexHeap, ({}, {}), queueOffset: {}, msgBlockHandle: {}",
                topic, queueId, queueOffset, msgBlockHandle);
        writeIndexHeap(topic, queueId, queueOffset, msgBlockHandle);

        topicQueueTable().putByPmem(topic, queueId, queueOffset, msgBlockHandle);

        return queueOffset;
    }


    // Data Structure:
    //  indexHeap: [dataBlock1, dataBlock2, ...]
    //      dataBlock: [currBlockWrotePosition, nextBlockHandle, indexItem1, indexItem2, ...]
    //      indexItem: (queueOffset, msgBlockHandle)
    //
    private void writeIndexHeap(String topic, int queueId, long queueOffset, long msgBlockHandleValue) {

        // find the index heap by topic+queueId

        // read from indexHeadBlock: get wrote position of index block
        //long rootAddr = indexHeap.getRoot();
        //MemoryBlock indexHeadBlock = indexHeap.memoryBlockFromHandle(rootAddr);
        //long wrotePosition = indexHeadBlock.getLong(0);
        //long indexBlockHandle = indexHeadBlock.getLong(8);

        // write into indexDataBlock: append indexItem to index block
        //MemoryBlock indexDataBlock = indexHeap.memoryBlockFromHandle(indexBlockHandle);
        //indexDataBlock.setLong(wrotePosition, queueOffset);
        //indexDataBlock.setLong(wrotePosition + 8, msgBlockHandleValue);
        //indexDataBlock.flush();
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
        //polish: reuse
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return bytes;
    }

    @Override
    public ByteBuffer getData(String topic, int queueId, long queueOffset) throws Exception {
        long pmemOffset = topicQueueTable().getPmemOffset(topic, queueId, queueOffset);
        MemoryBlock msgBlock = msgHeap.memoryBlockFromHandle(pmemOffset);
        int size = (int) msgBlock.size();
        byte[] bytes = new byte[size];
        msgBlock.copyToArray(0, bytes, 0, size);
        return byteArrayToByteBuffer(bytes);
    }

    private ByteBuffer byteArrayToByteBuffer(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        byteBuffer.clear();
        byteBuffer.put(bytes);

        byteBuffer.position(0);
        return byteBuffer;
    }

    public TopicQueueTable topicQueueTable() {
        return store.getTopicQueueTable();
    }
}
