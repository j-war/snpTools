package com.snptools.converter.vcfutilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for VcfToCsvTask.
 *  * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
@Disabled("Class is @Deprecated")
public class VcfToCsvTaskTest {

    final String TEST_INPUT_VCF = "./src/test/resources/test.vcf";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testVcf.csv";

    final int START_LINE_VCF = 11;
    final int END_LINE_VCF = 16;

    final int TOTAL_LINES = END_LINE_VCF - START_LINE_VCF;
    final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.
    final int COLUMN_WIDTH = 3; // <--- Depends on ploidiness. Diploid=3.

    /**
     * Tests whether the VcfToCsvTask worker was created successfully.
     * The VcfToCsvTask should be used on normalized data.
     */
    @Test
    @DisplayName("shouldConstructVcfToCsvTask")
    @Disabled("Class is @Deprecated")
    public void shouldConstructVcfToCsvTask() {
        VcfToCsvTask vcfToCsvTask = new VcfToCsvTask(TEST_INPUT_VCF, TEST_OUTPUT_VCF, START_COLUMN, END_COLUMN, NUMBER_OF_COLUMNS, TOTAL_LINES);
        assertNotNull(vcfToCsvTask);
    }

    /**
     * Tests whether a provided data entry is correctly parsed and interpreted.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResults")
    @Disabled("Class is @Deprecated")
    public void shouldAccumulateResults() throws NoSuchFieldException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException { 

        // Construct string[] of entries
        // Construct int[] of expected results
        // loop on calling accumulateResults()
        // Copy partialResults to testResults
        // Compare testResults to expected

        final String[] entriesToTest = new String[]{"here", "1/0", "0/0", "2/2", "3/0"}; // needs to TOTAL_LINES entries
        final int[] expectedResults = new int[]{5, 1, 0, 2, 1}; // Needs to have TOTAL_LINES entries.

        VcfToCsvTask vcfToCsvTask = new VcfToCsvTask(TEST_INPUT_VCF, TEST_OUTPUT_VCF, 0, 1, NUMBER_OF_COLUMNS, TOTAL_LINES);

        Method accumulateResults = VcfToCsvTask.class.getDeclaredMethod("accumulateResults", int.class, String.class);
        accumulateResults.setAccessible(true);

        for (int i = 0; i < entriesToTest.length; ++i) {
            accumulateResults.invoke(vcfToCsvTask, i, entriesToTest[i]);
        }

        Field partialResults = vcfToCsvTask.getClass().getDeclaredField("partialResults");
        partialResults.setAccessible(true);
        int[] testResults = (int[]) partialResults.get(vcfToCsvTask);

        // Check that results were obtained
        // Check that the expected number of results were obtained
        // Check that the interpretation of input entries was correct
        Assertions.assertAll(
            () -> assertTrue(testResults.length > 0),
            () -> assertTrue(expectedResults.length > 0),
            () -> assertEquals(expectedResults.length, testResults.length),
            () -> {
                for (int i = 0; i < entriesToTest.length; ++i) {
                    assertEquals(expectedResults[i], testResults[i]);
                }
            }
        );
    }

}
