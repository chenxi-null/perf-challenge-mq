package io.openmessaging.store;

import org.junit.jupiter.api.Test;

/**
 * @author chenxi20
 * @date 2021/10/21
 */
public class DataRecoveryTest extends BaseTest {

    @Test
    void canFindLastWrotePosition() {

        // given: write physical data1
        // and: sync logic data1
        // and: write physical data2
        // and: shutdown before sync logic data2

        // when: restart

        // then: check physical position - [data1, data2]
        // and: check logic position - [data1, data2]
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
