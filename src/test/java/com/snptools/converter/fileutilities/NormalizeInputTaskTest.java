package com.snptools.converter.fileutilities;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

    /**
     * Tests whether the NormalizeInputTask worker properly truncates
     * and stores data in partialResults.
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    @DisplayName("shouldAccumulateResults")
    public void shouldAccumulateResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        // Repeat for VCF and HMP:
        // vcf: startColumn = number of headers to skip, numberOfColumns = number of columns to keep, columnWidth = 3
        // hmp: startColumn = , numberOfColumns = , columnWidth = 
        // Create a test string to pass to accumulate()
        // create an expectedPartialResults array
        //
        // Create a Normalize worker
        // call accumulate with test string
        // ?copy its partialResults array?
        // Compare the partial results to expected results in a loop.

        NormalizeInputTask normalizeInputTask = new NormalizeInputTask(TEST_INPUT_PED, TEST_OUTPUT_PED, START_LINE, END_LINE, START_COLUMN, NUMBER_OF_COLUMNS, COLUMN_WIDTH);
        Method accumulateResults = NormalizeInputTask.class.getDeclaredMethod("accumulateResults", String.class);
        accumulateResults.setAccessible(true);
        accumulateResults.invoke(normalizeInputTask, "my test string");


    }

}
