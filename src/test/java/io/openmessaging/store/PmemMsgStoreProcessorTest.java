package io.openmessaging.store;

import io.openmessaging.store.pmem.IndexHeap;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi
 * @date 2021/10/29
 */
class PmemMsgStoreProcessorTest extends BaseTest {

    @Test
    void write() throws Exception {
        PmemMsgStoreProcessor p = getMQ().getStore().getPmemMsgStoreProcessor();
        IndexHeap indexHeap = getMQ().getStore().getIndexHeap();
        TopicQueueTable topicQueueTable = getMQ().getStore().getTopicQueueTable();

        String topic1 = "-topic-pmem-1";
        int queueId1 = 101;
        // when:
        p.write(topic1, queueId1, toByteBuffer("-content-pmem-test-1-"));

        // then:
        ByteBuffer data = p.getData(topic1, queueId1, 0);
        assertEquals("-content-pmem-test-1-", toString(data));

        // and: check indexHeap data
        TopicQueueTable tmpTable = new TopicQueueTable();
        indexHeap.load(tmpTable, topic1, queueId1);
        System.out.println("tmp: " + tmpTable);
        System.out.println("table: " + topicQueueTable);
        assertTrue(topicQueueTable.isSame(tmpTable));
    }
}
