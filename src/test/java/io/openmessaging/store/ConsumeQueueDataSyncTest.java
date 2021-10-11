package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/11
 */
class ConsumeQueueDataSyncTest extends BaseTest {

    @Test
    void dataSync() throws IOException {
        Config.getInstance().setEnableConsumeQueueDataSync(false);

        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        Store store = mq.getStore();
        ConsumeQueue consumeQueue = store.getConsumeQueue();
        TopicQueueTable memTable = store.getTopicQueueTable();

        // given:
        int msgNum = writeTestData(mq);
        assertEquals(msgNum, memTable.getMsgNum());
        assertEquals(0, consumeQueue.loadTopicQueueTable().getMsgNum());

        // when: invoke data sync
        store.getConsumeQueue().syncFromCommitLog();

        // then: consumeQueue finish data sync
        assertTrue(memTable.isSame(consumeQueue.loadTopicQueueTable()));

        // when: again write mq
        int msgNum2 = writeTestData2(mq);
        assertEquals(msgNum + msgNum2, memTable.getMsgNum());
        // and: again invoke data sync
        store.getConsumeQueue().syncFromCommitLog();
        // and: again assert
        assertTrue(memTable.isSame(consumeQueue.loadTopicQueueTable()));
    }
}
