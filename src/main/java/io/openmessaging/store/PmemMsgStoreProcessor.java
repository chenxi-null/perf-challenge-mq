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

    // pmem storage design:
    //
    // 'msg':
    // one heap, multi blocks: one msg one block
    //
    // 'index':
    // multi heaps:
    // one msg-queue one heap
    //  multi blocks in a heap
    //
    // write into msgHeap, create a new mem block, get block handle value
    // write the handle value into indexHeap

    private static final Logger log = LoggerFactory.getLogger(PmemMsgStoreProcessor.class);

    private final Store store;

    private Heap msgHeap;

    public PmemMsgStoreProcessor(Store store) {
        this.store = store;
    }

    public void start() throws IOException {
        FileUtil.createDirIfNotExists(Config.getInstance().getPmemDir());

        initMsgHeap();
    }

    private void initMsgHeap() {
        String path = Config.getInstance().getPmemMsgHeapPath();
        long heapSize = Config.getInstance().getPmemMsgHeapSize();
        this.msgHeap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
    }

    @Override
    public long write(String topic, int queueId, ByteBuffer data) throws Exception {
        log.trace("write ({}, {})", topic, queueId);
        long queueOffset = topicQueueTable().calcNextQueueOffset(topic, queueId);

        long msgBlockHandle = writeMsgHeap(data);

        log.trace("writeIndexHeap, ({}, {}), queueOffset: {}, msgBlockHandle: {}",
                topic, queueId, queueOffset, msgBlockHandle);
        writeIndexHeap(topic, queueId, queueOffset, msgBlockHandle);

        topicQueueTable().putByPmem(topic, queueId, queueOffset, msgBlockHandle);

        return queueOffset;
    }


    private void writeIndexHeap(String topic, int queueId, long queueOffset, long msgBlockHandleValue) throws IOException {
        store.getIndexHeap().write(topic, queueId, queueOffset, msgBlockHandleValue);
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
        if (pmemOffset <= 0) {
            return null;
        }
        MemoryBlock msgBlock = msgHeap.memoryBlockFromHandle(pmemOffset);
        int size = (int) msgBlock.size();
        byte[] bytes = new byte[size];
        msgBlock.copyToArray(0, bytes, 0, size);
        return byteArrayToByteBuffer(bytes);
    }

    private ByteBuffer byteArrayToByteBuffer(byte[] bytes) {
        //polish: reuse
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
