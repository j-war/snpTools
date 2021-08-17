package com.snptools.converter.pedutilities;

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
 * A set of unit tests for PedResultsTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
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

    /**
     * Tests whether a read line from the input file is correctly
     * parsed and interpreted.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResults")
    public void shouldAccumulateResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
        // 1. Create a major alleles array
        // 2. Create a string to pass in
        // 3. Create an expected results array
        // 4. Run accumulateResults()
        // 5. >>> Compare the partial results array to the expected to confirm the test.

        // *** accumulateResults() in PedResultsTask compares TWO inputs from the line to a single entry in the majorAlleles array.

        // 1.
        // majoralleles = 3093, the number of input columns
        // Create a majorAlleles array here to pass to PedResultsTask
        final String[] majorAlleles = new String[]{"A", "A", "C", "C", "T", "T", "G", "G", "A", "C"};

        // 2. Construct the test line:
        // reads in an entire ped line: 3093*2 + line header (6 columns) = 6192 entries
        final String lineHeader = "col1\tcol2\tcol3\tcol4\tcol5\tcol6\t";
        final String dataLine = "A\tC\tT\tG\tA\tC\tT\tG\tA\tC\tT\tG\tA\tC\tT\tG\t0\tG\tC\t0";
        // This should have twice as many entries as majorAlleles array.
        //                         a     a     c     c     t     t     g     g     a     c
        //                         1     2     1     2     2     1     2     1     5     4
        // 0 occurs when both pairs are majors.
        // 1 occurs when one is a major and one is a minor.
        // 2 occurs when both are minors.
        // 3 does not appear in the csv scheme.
        // 4 occurs when the first allele in a pair is a major and the second is 0/x/unknown.
        // 5 occurs when the both pairs of alleles are 0/x/unknown.
        final String testLine = lineHeader + dataLine + "\n";

        // 3.
        // From the line in 2, we can manually create an array of expected results
        // The same size as majorAlleles, 3093:
        final int[] expectedResults = new int[]{1, 2, 1, 2, 2, 1, 2, 1, 5, 4};

        // 4. Reflection - set up to call accumulateResults() with prepared test data:
        PedResultsTask pedResultsTask = new PedResultsTask(TEST_INPUT_PED, TEST_OUTPUT_PED, 0, 1, majorAlleles.length, majorAlleles);

        Method accumulateResults = PedResultsTask.class.getDeclaredMethod("accumulateResults", int.class, String.class);
        accumulateResults.setAccessible(true);
        accumulateResults.invoke(pedResultsTask, 0, testLine);

        Field partialResults = pedResultsTask.getClass().getDeclaredField("partialResults");
        partialResults.setAccessible(true);
        int[] testResults = (int[]) partialResults.get(pedResultsTask);

        // 5. Compare the arrays of step 4 (actual) and step 3 (expected):
        // Check that there were testResults obtained by verifying lengths > 0.
        // Check that the testResults match the expectedResults
        Assertions.assertAll(
            () -> assertTrue(testResults.length > 0),
            () -> assertTrue(expectedResults.length > 0),
            () -> assertEquals(expectedResults.length, testResults.length),
            () -> {
                for (int i = 0; i < testResults.length; ++i) {
                    assertEquals(expectedResults[i], testResults[i]);
                }
            }
        );

    }

}
