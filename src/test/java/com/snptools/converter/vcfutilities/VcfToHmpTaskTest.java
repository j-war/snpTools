package com.snptools.converter.vcfutilities;

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
 * A set of unit tests for VcfToHmpTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class VcfToHmpTaskTest {

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

    final String[] alleles = new String[NUMBER_OF_COLUMNS];
    final String[] outputLineHeaders = new String[NUMBER_OF_COLUMNS];

    /**
     * Tests whether the VcfToHmpTask worker was created successfully.
     * The VcfToHmpTask should be used on normalized data.
     */
    @Test
    @DisplayName("shouldConstructVcfToHmpTask")
    public void shouldConstructVcfToHmpTask() {
        VcfToHmpTask vcfToHmpTask = new VcfToHmpTask(TEST_INPUT_VCF, TEST_OUTPUT_VCF, START_LINE_VCF, END_LINE_VCF, NUMBER_OF_COLUMNS, alleles, outputLineHeaders);
        assertNotNull(vcfToHmpTask);
    }

    /**
     * Tests whether a provided input line is correctly parsed and interpreted, including
     * alphabetizing HMP output.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateResults")
    public void shouldAccumulateResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException { 

        // Construct an alleles array too - needs NUMBER_OF_COLUMNS entries.
        // Construct string[] of entries
        // Construct int[] of expected results
        //    Loop on calling accumulateResults(lineNumber, j=totalColumns, entry=3 chars)
        //                      String[] values = alleles[lineNumber].split(",");
        // Copy partialResults to testResults
        // Compare testResults to expected

        // One entry in testAlleles PER line of entriesToTest:
        final String[] testAlleles = new String[]{"A,C,G", "A,T", "A,G", "G,A,T"}; // length = NUMBER_OF_COLUMNS

        final String[] entriesToTestOne = new String[]{"0/0", "1/0", "0/1", "0/1", "1/0", "0/0"};
        final String[] expectedResultsOne = new String[]{"AA", "AC", "AC", "AC", "AC", "AA"};

        final String[] entriesToTestTwo = new String[]{"1/1", "1/0", "0/1", "0/2", "./.", "0/0"};
        final String[] expectedResultsTwo = new String[]{"TT", "AT", "AT", "NN", "NN", "AA"};

        final String[] entriesToTestThree = new String[]{"0/0", "1/0", "0/1", "1/1", "2/0", "0/0"};
        final String[] expectedResultsThree = new String[]{"AA", "AG", "AG", "GG", "NN", "AA"};

        final String[] entriesToTestFour = new String[]{"./.", "1/0", "0/1", "0/1", "1/0", "0/2"};
        final String[] expectedResultsFour = new String[]{"NN", "AG", "AG", "AG", "AG", "GT"};

        VcfToHmpTask vcfToHmpTask = new VcfToHmpTask(TEST_INPUT_VCF, TEST_OUTPUT_VCF, START_LINE_VCF, END_LINE_VCF, expectedResultsOne.length, testAlleles, outputLineHeaders);

        Method accumulateResults = VcfToHmpTask.class.getDeclaredMethod("accumulateResults", int.class, int.class, String.class);
        accumulateResults.setAccessible(true);

        Field partialResults = vcfToHmpTask.getClass().getDeclaredField("partialResults");
        partialResults.setAccessible(true);

        System.out.println("\nNote: Ignore \"Skipping input ... input data appears inconsistent.\" print statements while testing.\n");
        // 1.
        for (int j = 0; j < entriesToTestOne.length; ++j) {
            accumulateResults.invoke(vcfToHmpTask, 0, j, entriesToTestOne[j]); 
        }
        String[] testResultsOne = (String[]) partialResults.get(vcfToHmpTask);
        // Check that results were obtained
        // Check that the expected number of results were obtained
        // Check that the interpretation of input entries was correct
        Assertions.assertAll(
            () -> assertTrue(testResultsOne.length > 0),
            () -> assertTrue(expectedResultsOne.length > 0),
            () -> assertEquals(expectedResultsOne.length, testResultsOne.length),
            () -> {
                for (int k = 0; k < entriesToTestOne.length; ++k) {
                    assertEquals(expectedResultsOne[k], testResultsOne[k]);
                }
            }
        );

        // 2.
        for (int j = 0; j < entriesToTestTwo.length; ++j) {
            accumulateResults.invoke(vcfToHmpTask, 1, j, entriesToTestTwo[j]); 
        }
        String[] testResultsTwo = (String[]) partialResults.get(vcfToHmpTask); // Update test results
        Assertions.assertAll(
            () -> assertTrue(testResultsTwo.length > 0),
            () -> assertTrue(expectedResultsTwo.length > 0),
            () -> assertEquals(expectedResultsTwo.length, testResultsTwo.length),
            () -> {
                for (int k = 0; k < entriesToTestTwo.length; ++k) {
                    assertEquals(expectedResultsTwo[k], testResultsTwo[k]);
                }
            }
        );

        // 3.
        for (int j = 0; j < entriesToTestThree.length; ++j) {
            accumulateResults.invoke(vcfToHmpTask, 2, j, entriesToTestThree[j]); 
        }
        String[] testResultsThree = (String[]) partialResults.get(vcfToHmpTask); // Update test results
        Assertions.assertAll(
            () -> assertTrue(testResultsThree.length > 0),
            () -> assertTrue(expectedResultsThree.length > 0),
            () -> assertEquals(expectedResultsThree.length, testResultsThree.length),
            () -> {
                for (int k = 0; k < entriesToTestThree.length; ++k) {
                    assertEquals(expectedResultsThree[k], testResultsThree[k]);
                }
            }
        );

        // 4.
        for (int j = 0; j < entriesToTestFour.length; ++j) {
            accumulateResults.invoke(vcfToHmpTask, 3, j, entriesToTestFour[j]); 
        }
        String[] testResultsFour = (String[]) partialResults.get(vcfToHmpTask); // Update test results
        Assertions.assertAll(
            () -> assertTrue(testResultsFour.length > 0),
            () -> assertTrue(expectedResultsFour.length > 0),
            () -> assertEquals(expectedResultsFour.length, testResultsFour.length),
            () -> {
                for (int k = 0; k < entriesToTestFour.length; ++k) {
                    assertEquals(expectedResultsFour[k], testResultsFour[k]);
                }
            }
        );

    }

}
