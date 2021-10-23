package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author chenxi20
 * @date 2021/10/21
 */
public class DataRecoveryTest extends BaseTest {

    @Test
    void canFindLastWrotePosition() throws IOException, InterruptedException {
        Config.getInstance().setEnableConsumeQueueDataSync(false);
        DefaultMessageQueueImpl mq = getMQ();
        Store store = mq.getStore();
        ConsumeQueue consumeQueue = mq.getStore().getConsumeQueue();

        // given: write physical data1
        int logNum1 = writeTestData(mq);
        // and: sync logic data1
        consumeQueue.syncFromCommitLog();
        assertEquals(logNum1, consumeQueue.loadTopicQueueTable().getMsgNum());
        // and: write physical data2
        int logNum2 = writeTestData2(mq);
        // and: shutdown before sync logic data2
        assertEquals(logNum1, consumeQueue.loadTopicQueueTable().getMsgNum());
        mq.stop();

        // when: restart
        mq = new DefaultMessageQueueImpl();

        // then: check physical position - [data1, data2]
        // and: check logic position - [data1, data2]
        assertEquals(logNum1 + logNum2, consumeQueue.loadTopicQueueTable().getMsgNum());
    }

    @Test
    void canProcessPhysicalTailDirtyData() {
        // given: write physical data1
        // and: sync logic data1
        // and: write physical data2 with dirty tail

        // when: restart
        // then: check physical position - [data1, data2]
        // and: check logic position - [data1, data2]
    }

    @Test
    void canProcessLogicTailDirtyData() {
        // given: write physical data1
        // and: sync logic data1
        // and: write physical data2
        // and: sync logic data2 with dirty tail

        // when: restart
        // then: check physical position - [data1, data2]
        // then: check logic position - [data1, data2]
    }
}
