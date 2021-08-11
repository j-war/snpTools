package com.snptools.converter.fileutilities;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for NormalizeInputTask.
 */
public class NormalizeInputTaskTest {

    // test.ped has 474 * 2 data entries = 948.
    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";
    final int START_LINE = 0;
    final int END_LINE = 3;

    final int START_COLUMN = 6; // Where this worker should start.
    final int NUMBER_OF_COLUMNS = 10; // The number of columns that should be kept by this worker.
    final int COLUMN_WIDTH = 3; // <--- Depends on ploidiness. Diploid=3.

    /**
     * Tests whether the NormalizeInputTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructNormalizeInputTask")
    public void shouldConstructNormalizeInputTask() {
        NormalizeInputTask normalizeInputTask = new NormalizeInputTask(TEST_INPUT_PED, TEST_OUTPUT_PED, START_LINE, END_LINE, START_COLUMN, NUMBER_OF_COLUMNS, COLUMN_WIDTH);
        assertNotNull(normalizeInputTask);
    }

}
