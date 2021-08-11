package com.snptools.converter.pedutilities;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for PedResultsTask.
 */
public class PedResultsTaskTest {

    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";
    final int START_LINE = 0;
    final int END_LINE = 3;
    final int COLUMN_PAIRS = 474; // is equal to half the number of data columns in the test file.
    final String[] ALLELES = new String[10];

    /**
     * Tests whether the PedResultsTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructPedResultsTask")
    public void shouldConstructPedResultsTask() {
        PedResultsTask pedResultsTask = new PedResultsTask(TEST_INPUT_PED, TEST_OUTPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS, ALLELES);
        assertNotNull(pedResultsTask);
    }
}
