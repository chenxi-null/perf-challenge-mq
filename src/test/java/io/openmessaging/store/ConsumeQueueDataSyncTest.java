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

        // again write mq
        // again invoke data sync
        // again assert
    }
}
