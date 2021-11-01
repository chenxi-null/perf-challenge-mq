package io.openmessaging.store.pmem;

import io.openmessaging.store.BaseTest;
import io.openmessaging.store.TopicQueueTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author chenxi20
 * @date 2021/10/31
 */
class IndexHeapTest extends BaseTest {

    @Test
    void test() throws IOException {
        IndexHeap indexHeap = getMQ().getStore().getIndexHeap();
        TopicQueueTable topicQueueTable = getMQ().getStore().getTopicQueueTable();

        String topic1 = "-topic-1-";
        int queueId1 = 1101;
        int msgBlockHandle = 123;

        indexHeap.write(topic1, queueId1, 0, msgBlockHandle);
        indexHeap.load(topicQueueTable, topic1, queueId1);
        assertEquals(msgBlockHandle, topicQueueTable.getPmemOffset(topic1, queueId1, 0));
    }
}
