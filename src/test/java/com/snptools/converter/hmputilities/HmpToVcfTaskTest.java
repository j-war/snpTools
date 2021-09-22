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
 * A set of unit tests for HmpToVcfTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class HmpToVcfTaskTest {

    final String TEST_INPUT_HMP = "./src/test/resources/testHalfNormal.hmp";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testHmp.vcf";

    final int START_LINE_HMP = 1;
    final int END_LINE_HMP = 6;

    final int TOTAL_LINES = END_LINE_HMP - START_LINE_HMP;
    final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.
    final int COLUMN_WIDTH = 2; // <--- Depends on ploidiness. Diploid=2.

    final int NUMBER_OF_BASES_TO_SUM = 2 * (NUMBER_OF_COLUMNS);

    final String[] majorAlleles = new String[]{"A,C,T,G", "A,C", "C,T", "C,G,T", "T,A,C,N,G"}; //, "T,A,G", "G,T", "G,A", "A,C,T,G,N", "C,G,T"};
    final String[] strandDirections = new String[]{"+", "+", "+", "-", "+"}; //, "+", "+", "+", "+", "+"};
    final String[] outputLineHeaders = new String[]{};

    /**
     * Tests whether the HmpToVcfTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructHmpToVcfTask")
    public void shouldConstructHmpToVcfTask() {
        /* HmpToVcfTask()
        String inputFilename
        String outputFilename
        int startLine
        int endLine
        int totalColumns
        String[] alleles
        String[] strandDirections
        String[] outputLineHeaders
        */
        HmpToVcfTask hmpToVcfTask = new HmpToVcfTask(
            TEST_INPUT_HMP, TEST_OUTPUT_VCF,
            START_LINE_HMP, END_LINE_HMP, NUMBER_OF_COLUMNS,
            majorAlleles, strandDirections, outputLineHeaders, 1);
        assertNotNull(hmpToVcfTask);
    }

    /**
     * Tests whether an array of entries from the input file is correctly parsed and
     * interpreted.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResultsDiploid")
    public void shouldAccumulateResultsDiploid() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        // 1. Prepare an array of entries and expectedResults.
        //     - test data should be normalized.
        // 2. Create an HmpToVcfTask worker.
        // 3. Loop on accumulate accumulateTotals() with prepared data.
        // 4. Use reflection to copy partialResults to testResults.
        // 5. Loop and compare expected and actual.

        // 1.
        final int NUMBER_OF_TEST_COLUMNS = 10; // The number of HMP entries in the dataLines below.
        final int NUMBER_OF_TEST_LINES = 5; // Number of test lines to create.
        final int ploidiness = 2; // Diploid.

        final String[] dataLineOne = new String[]       {"aa",  "CC",  "TT",  "gG",  "AT",  "TG",  "GT",  "GA",  "NN",  "TT"};// "A,C,T,G"
        final String[] expectedResultsOne = new String[]{"0/0", "1/1", "2/2", "3/3", "0/2", "2/3", "3/2", "3/0", "./.", "2/2"};

        final String[] dataLineTwo = new String[]       {"TT",  "CC",  "TT",  "CC",  "AA",  "TT",  "Gt",  "AA",  "AC",  "CA"}; // "A,C"
        final String[] expectedResultsTwo = new String[]{"./.", "1/1", "./.", "1/1", "0/0", "./.", "./.", "0/0", "0/1", "1/0"};

        final String[] dataLineThree = new String[]       {"CG",  "AC",  "CC",  "GC",  "TC",  "GA",  "GT",  "GA",  "GT",  "TC"}; // "C,T"
        final String[] expectedResultsThree = new String[]{"0/.", "./0", "0/0", "./0", "1/0", "./.", "./1", "./.", "./1", "1/0"};

        // Note: This testLine is "-":
        final String[] dataLineFour = new String[]       {"TT",  "CC",  "TT",  "CC",  "AA",  "TT",  "Gt",  "AA",  "NN",  "TG"}; // "C,G,T"
        final String[] expectedResultsFour = new String[]{"2/2", "0/0", "2/2", "0/0", "./.", "2/2", "2/1", "./.", "./.", "1/2"};

        final String[] dataLineFive = new String[]       {"CG",  "AC",  "CC",  "GC",  "AA",  "GA",  "GT",  "GA",  "GT",  "TC"}; // "T,A,C,N,G"
        final String[] expectedResultsFive = new String[]{"2/4", "1/2", "2/2", "4/2", "1/1", "4/1", "4/0", "4/1", "4/0", "0/2"};

        final String[][] dataLines = new String[][]{dataLineOne, dataLineTwo, dataLineThree, dataLineFour, dataLineFive};
        final String[][] expectedResults = new String[][]{expectedResultsOne, expectedResultsTwo, expectedResultsThree, expectedResultsFour, expectedResultsFive};

        // 2.
        HmpToVcfTask hmpToVcfTask = new HmpToVcfTask(
            TEST_INPUT_HMP,
            TEST_OUTPUT_VCF,
            START_LINE_HMP,
            END_LINE_HMP,
            NUMBER_OF_TEST_COLUMNS,
            majorAlleles,
            strandDirections,
            outputLineHeaders,
            ploidiness
        );

        // 3a.
        Method accumulateResultsMethodOne = HmpToVcfTask.class.getDeclaredMethod("accumulateResults", int.class, int.class, String.class);
        accumulateResultsMethodOne.setAccessible(true);

        // 3b. Loop over assertions with test data:
        for (int i = 0; i < NUMBER_OF_TEST_LINES; ++i) {
            final int iCopy = i; // Make and use the final copy for use inside lambda below.
            // 3b.
            for (int j = 0; j < dataLines[iCopy].length; ++j) {//NUMBER_OF_TEST_COLUMNS
                accumulateResultsMethodOne.invoke(hmpToVcfTask, iCopy, j, dataLines[iCopy][j]);
                //accumulateResultsMethodOne.invoke(hmpToVcfTask, 0, j, dataLineOne[j]);
            }

            // 4.
            Field partialResults = hmpToVcfTask.getClass().getDeclaredField("partialResults");
            partialResults.setAccessible(true);
            String[] testResultsOne = (String[]) partialResults.get(hmpToVcfTask);

            // 5.
            // Check that results were obtained
            // Check that the expected number of results were obtained
            // Check that the interpretation of input entries was correct
            Assertions.assertAll(
                () -> assertTrue((expectedResults[iCopy]).length > 0),
                () -> assertTrue(testResultsOne.length == NUMBER_OF_TEST_COLUMNS),
                () -> assertEquals((expectedResults[iCopy]).length, testResultsOne.length),
                () -> {
                    for (int j = 0; j < (expectedResults[iCopy]).length; ++j) {
                        assertEquals((expectedResults[iCopy])[j], testResultsOne[j]);
                    }
                }
            );
        }

    }

    /**
     * Tests whether an array of entries from the input file is correctly parsed and
     * interpreted.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResultsHaploid")
    public void shouldAccumulateResultsHaploid() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        // 1.
        final int NUMBER_OF_TEST_COLUMNS = 10; // The number of HMP entries in the dataLines below.
        final int NUMBER_OF_TEST_LINES = 5; // Number of test lines to create.
        final int ploidiness = 1; // Haploid.

        final String[] dataLineOneSingle = new String[]       {"a", "C", "T", "g", "A", "T", "G", "A", "N", "T"};// "A,C,T,G"
        final String[] expectedResultsOneSingle = new String[]{"0", "1", "2", "3", "0", "2", "3", "0", ".", "2"};

        final String[] dataLineTwoSingle = new String[]       {"T", "C", "T", "C", "A", "T", "t", "a", "C", "A"}; // "A,C"
        final String[] expectedResultsTwoSingle = new String[]{".", "1", ".", "1", "0", ".", ".", "0", "1", "0"};

        final String[] dataLineThreeSingle = new String[]       {"C", "C", "C", "G", "T", "G", "G", "A", "T", "T"}; // "C,T"
        final String[] expectedResultsThreeSingle = new String[]{"0", "0", "0", ".", "1", ".", ".", ".", "1", "1"};

        // Note: This testLine is "-":
        final String[] dataLineFourSingle = new String[]       {"T", "C", "T", "C", "A", "T", "t", "A", "N", "G"}; // "C,G,T"
        final String[] expectedResultsFourSingle = new String[]{"2", "0", "2", "0", ".", "2", "2", ".", ".", "1"};

        final String[] dataLineFiveSingle = new String[]       {"G", "A", "C", "G", "A", "A", "T", "G", "T", "C"}; // "T,A,C,N,G"
        final String[] expectedResultsFiveSingle = new String[]{"4", "1", "2", "4", "1", "1", "0", "4", "0", "2"};

        final String[][] dataLinesSingle = new String[][]{dataLineOneSingle, dataLineTwoSingle, dataLineThreeSingle, dataLineFourSingle, dataLineFiveSingle};
        final String[][] expectedResultsSingle = new String[][]{expectedResultsOneSingle, expectedResultsTwoSingle, expectedResultsThreeSingle, expectedResultsFourSingle, expectedResultsFiveSingle};

        // 2.
        HmpToVcfTask hmpToVcfTaskSingle = new HmpToVcfTask(
            TEST_INPUT_HMP,
            TEST_OUTPUT_VCF,
            START_LINE_HMP,
            END_LINE_HMP,
            NUMBER_OF_TEST_COLUMNS,
            majorAlleles,
            strandDirections,
            outputLineHeaders,
            ploidiness
        );

        // 3a.
        Method accumulateResultsMethodSingle = HmpToVcfTask.class.getDeclaredMethod("accumulateResults", int.class, int.class, String.class);
        accumulateResultsMethodSingle.setAccessible(true);

        // 3b. Loop over assertions with test data:
        for (int i = 0; i < NUMBER_OF_TEST_LINES; ++i) {
            final int iCopy = i; // Make and use the final copy for use inside lambda below.
            // 3b.
            for (int j = 0; j < (dataLinesSingle[iCopy]).length; ++j) {//NUMBER_OF_TEST_COLUMNS
                accumulateResultsMethodSingle.invoke(hmpToVcfTaskSingle, iCopy, j, dataLinesSingle[iCopy][j]);
                //accumulateResultsMethodTwo.invoke(hmpToVcfTaskTwo, 0, j, dataLineOne[j]);
            }

            // 4.
            Field partialResults = hmpToVcfTaskSingle.getClass().getDeclaredField("partialResults");
            partialResults.setAccessible(true);
            String[] testResultsSingle = (String[]) partialResults.get(hmpToVcfTaskSingle);

            // 5.
            // Check that results were obtained
            // Check that the expected number of results were obtained
            // Check that the interpretation of input entries was correct
            Assertions.assertAll(
                () -> assertTrue((expectedResultsSingle[iCopy]).length > 0),
                () -> assertTrue(testResultsSingle.length == NUMBER_OF_TEST_COLUMNS),
                () -> assertEquals((expectedResultsSingle[iCopy]).length, testResultsSingle.length),
                () -> {
                    for (int j = 0; j < (expectedResultsSingle[iCopy]).length; ++j) {
                        assertEquals((expectedResultsSingle[iCopy])[j], testResultsSingle[j]);
                    }
                }
            );
        }

    }

}
