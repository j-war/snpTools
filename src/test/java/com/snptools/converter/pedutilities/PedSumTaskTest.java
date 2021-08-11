package com.snptools.converter.pedutilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for PedSumTask.
 */
public class PedSumTaskTest {

    // test.ped has 474 * 2 data entries = 948.
    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final int START_LINE = 0;
    final int END_LINE = 3;
    final int COLUMN_PAIRS = 474; // is equal to half the number of data columns.
    final int NUMBER_OF_BASES_TO_SUM = 2 * (END_LINE - START_LINE);

    /**
     * Tests whether the PedResultsTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructPedSumTask")
    public void shouldConstructPedSumTask() {
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS);
        assertNotNull(pedSumTask);
    }

    /**
     * Tests whether the totals datastructure was initialized and
     * filled correctly by checking that the sums are consistent.
     */
    @Test
    @DisplayName("testInitAndCollectTotals")
    void testInitAndCollectTotals() throws InterruptedException {
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS);
        Thread task = new Thread(pedSumTask);
        task.start();
        task.join();

        Assertions.assertAll(
            () -> assertEquals(COLUMN_PAIRS, (pedSumTask.getTotals()).size()),
            () -> {
                for (int i = 0; i < COLUMN_PAIRS; ++i) {
                    assertEquals(NUMBER_OF_BASES_TO_SUM, (
                        (pedSumTask.getTotals()).get(i)[0]
                        + (pedSumTask.getTotals()).get(i)[1]
                        + (pedSumTask.getTotals()).get(i)[2]
                        + (pedSumTask.getTotals()).get(i)[3]
                        + (pedSumTask.getTotals()).get(i)[4]
                        ));
                }
            }
        );
    }

}
