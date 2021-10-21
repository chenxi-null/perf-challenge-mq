package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.common.NamedThreadFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author chenxi20
 * @date 2021/10/19
 */
@Disabled
public class PerfTest extends BaseTest {

    private static final Logger log = LoggerFactory.getLogger(PerfTest.class);

    @Test
    void concurrentWriteAndRead() throws InterruptedException {
        Config.getInstance().setOneWriteMaxDataSize(10 * 1024);

        DefaultMessageQueueImpl mq = getMQ();

        CountDownLatch latch = new CountDownLatch(1);
        Random random = new Random();

        WaitFlag waitFlag = new WaitFlag();

        Runnable readCmd = () -> {
            String topic = "topic-" + random.nextInt(5);
            int queueId = 1000 + random.nextInt(5);
            int starOffset = random.nextInt(3);
            int num = random.nextInt(100);

            if (waitFlag.value) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            mq.getRange(topic, queueId, starOffset, num);
        };

        Runnable writeCmd = () -> {
            String topic = "topic-" + random.nextInt(5);
            int queueId = 1000 + random.nextInt(5);
            int size = 800 + random.nextInt(5 * 1024);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size; i++) {
                sb.append('%');
            }
            String data = "[" + sb + "]";

            if (waitFlag.value) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            mq.append(topic, queueId, toByteBuffer(data));
        };

        waitFlag.value = false;
        for (int i = 0; i < 500; i++) {
            wrap(writeCmd).run();
        }
        waitFlag.value = true;

        System.out.println(">>> finish data prepare ----");

        ExecutorService executor = Executors.newFixedThreadPool(100, new NamedThreadFactory("t"));
        for (int i = 0; i < 50; i++) {
            executor.submit(wrap(writeCmd));
        }
        for (int i = 0; i < 50; i++) {
            executor.submit(wrap(readCmd));
        }

        latch.countDown();

        boolean ok = executor.awaitTermination(30, TimeUnit.SECONDS);
        //assertTrue(ok, "thread pool stp");
    }

    Runnable wrap(Runnable r) {
        return () -> {
            try {
                r.run();
            } catch (Throwable e) {
                log.error("thread occur ex", e);
            }
        };
    }

    static class WaitFlag {
        volatile boolean value;
    }
}
