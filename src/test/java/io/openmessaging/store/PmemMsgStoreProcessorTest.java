package io.openmessaging.store;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author chenxi20
 * @date 2021/10/29
 */
class PmemMsgStoreProcessorTest extends BaseTest {

    @Test
    void write() throws Exception {

        PmemMsgStoreProcessor p = getMQ().getStore().getPmemMsgStoreProcessor();

        try {
            p.write("-topic-pmem-1", 101, toByteBuffer("-content-pmem-test-1-"));
            ByteBuffer data = p.getData("-topic-pmem-1", 101, 0);
            assertEquals("-content-pmem-test-1-", toString(data));
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
