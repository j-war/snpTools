package com.snptools.converter.hmputilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for HmpToCsvTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class HmpToCsvTaskTest {

    final String TEST_INPUT_HMP = "./src/test/resources/testHalfNormal.hmp";
    final String TEST_OUTPUT_HMP = "./src/test/resources/testHmp.csv";

    final int START_LINE_HMP = 1;
    final int END_LINE_HMP = 6;

    final int TOTAL_LINES = END_LINE_HMP - START_LINE_HMP;
    final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.
    final int COLUMN_WIDTH = 2; // <--- Depends on ploidiness. Diploid=2.

    final int NUMBER_OF_BASES_TO_SUM = 2 * (NUMBER_OF_COLUMNS);
    final int NUMBER_OF_BASES = 5; // ACTG0

    final String[] majorAlleles = new String[]{"A", "A", "C", "C", "T", "T", "G", "G", "A", "C"};

    /**
     * Tests whether the HmpToCsvTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructHmpToCsvTask")
    public void shouldConstructHmpToCsvTask() {
        HmpToCsvTask hmpToCsvTask = new HmpToCsvTask(TEST_INPUT_HMP, TEST_INPUT_HMP, START_COLUMN, END_COLUMN, NUMBER_OF_COLUMNS, TOTAL_LINES, majorAlleles, 1);
        assertNotNull(hmpToCsvTask);
    }

    /**
     * Tests whether an array of entries from the input file is correctly parsed and
     * interpreted.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResults")
    public void shouldAccumulateResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        // 1. Prepare an array of entries and expectedResults.
        //     - test data should be normalized.
        // 2. Create an HmpToCsvTask worker.
        // 3. Loop on accumulate accumulateTotals() with prepared data.
        // 4. Use reflection to copy partialResults to testResults.
        // 5. Loop and compare expected and actual.

        // 1.
        final int NUMBER_OF_TEST_COLUMNS = 10; // The number of HMP entries in the dataLines below.
        final int NUMBER_OF_TESTLINES = 3;

        final String[] dataLineOne = new String[]{"aa", "CC", "TT", "gG", "AC", "TG", "CT", "GT", "NN", "TT"};
        final String[] dataLineTwo = new String[]{"NN", "GT", "GG", "TT", "AA", "TT", "Gt", "TT", "NN", "TG"};
        final String[] dataLineThree = new String[]{"CG", "GC", "nN", "00", "NN", "GA", "GA", "GC", "GT", "NN"};
        final String[][] dataLines = new String[][]{dataLineOne, dataLineTwo, dataLineThree};

        final int[] expectedResultsOne = new int[]{0, 2, 2, 2, 2, 1, 2, 1, 2, 2};
        final int[] expectedResultsTwo = new int[]{2, 2, 2, 2, 2, 0, 1, 2, 2, 2};
        final int[] expectedResultsThree = new int[]{2, 2, 2, 5, 2, 2, 1, 1, 2, 2};
        final int[][] expectedResults = new int[][]{expectedResultsOne, expectedResultsTwo, expectedResultsThree};

        // 2.
        HmpToCsvTask hmpToCsvTask = new HmpToCsvTask(TEST_INPUT_HMP, TEST_INPUT_HMP, START_COLUMN, END_COLUMN, NUMBER_OF_TEST_COLUMNS, NUMBER_OF_TEST_COLUMNS, majorAlleles, 2);

        // 3a.
        Method accumulateResultsMethod = HmpToCsvTask.class.getDeclaredMethod("accumulateResults", int.class, String.class);
        accumulateResultsMethod.setAccessible(true);

        // 3b. Loop over assertions with test data:
        for (int j = 0; j < NUMBER_OF_TESTLINES; ++j) {
            final int jCopy = j; // Make a final copy for use inside lambda below.
            // 3b.
            for (int i = 0; i < NUMBER_OF_TEST_COLUMNS; ++i) {
                accumulateResultsMethod.invoke(hmpToCsvTask, i, dataLines[jCopy][i]);
            }

            // 4.
            Field partialResults = hmpToCsvTask.getClass().getDeclaredField("partialResults");
            partialResults.setAccessible(true);
            int[] testResults = (int[]) partialResults.get(hmpToCsvTask);

            // 5.
            // Check that results were obtained
            // Check that the expected number of results were obtained
            // Check that the interpretation of input entries was correct
            Assertions.assertAll(
                () -> assertTrue(expectedResults[jCopy].length > 0),
                () -> assertTrue(testResults.length == NUMBER_OF_TEST_COLUMNS),
                () -> assertEquals((expectedResults[jCopy]).length, testResults.length),
                () -> {
                    for (int i = 0; i < (expectedResults[jCopy]).length; ++i) {
                        assertEquals((expectedResults[jCopy])[i], testResults[i]);
                    }
                }
            );
        }
    }

}
