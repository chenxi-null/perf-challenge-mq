package io.openmessaging.store;

import java.nio.ByteBuffer;

/**
 * @author chenxi20
 * @date 2021/10/28
 */
public interface MsgStoreProcessor {

    /**
     * @return queueOffset
     */
    long write(String topic, int queueId, ByteBuffer data) throws Exception;


    ByteBuffer getData(String topic, int queueId, long offset) throws Exception;
}
