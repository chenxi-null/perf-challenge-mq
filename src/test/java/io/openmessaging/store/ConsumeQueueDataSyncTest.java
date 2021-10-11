package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import org.junit.jupiter.api.Test;

/**
 * @author chenxi20
 * @date 2021/10/11
 */
class ConsumeQueueDataSyncTest extends BaseTest {

    @Test
    void dataSync() {
        Config.getInstance().setEnableConsumeQueueDataSync(false);

        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        Store store = mq.getStore();

        writeTestData(mq);
        //store.getTopicQueueTable();

        // invoke data sync

        // read all data from consumeQueue
        // compare with in-mem topicQueueTable

        // again write mq
        // again invoke data sync
        // again assert
    }
}
