package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.util.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author chenxi20
 * @date 2021/10/29
 */
class PmemMsgStoreProcessorTest extends BaseTest {

    @Test
    void write() throws Exception {
        Config.getInstance().setPmemMsgHeapSize(1024);
        String pmemDir = new File("./output/pmem").getCanonicalPath();
        Config.getInstance().setPmemDir(pmemDir);
        assertTrue(FileUtil.safeDeleteDirectory(pmemDir));
        System.out.println("clear pmemDir: " + pmemDir);

        PmemMsgStoreProcessor p = getMQ().getStore().getPmemMsgStoreProcessor();
        //System.out.println(">>> p: " + p);

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
