package com.snptools.converter.hmputilities;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for HmpToCsvTaskLargeTest.
 * 
 * @author  Jeff Warner
 * @version 1.0, September 2021
 */
public class HmpToCsvTaskLargeTest {

    final String TEST_INPUT_HMP = "./src/test/resources/testFullNormal.hmp";
    final String TEST_OUTPUT_HMP = "./src/test/resources/testHmp.csv";
    final String TEST_OUTPUT_HMP_LARGE = "./src/test/resources/LARGEtestHmp.csv"; // Does not exist - placeholder value.

    final int START_LINE_HMP = 0;
    final int END_LINE_HMP = 5;

    final int TOTAL_LINES = END_LINE_HMP - START_LINE_HMP;
    //final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.

    final String[] majorAlleles = new String[]{"N", "A", "C", "C", "T", "T", "G", "G", "N", "N"};

    /**
     * Tests whether the HmpToCsvTaskLarge worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructHmpToCsvTaskLarge")
    public void shouldConstructHmpToCsvTaskLarge() {
        final int portion = 1;
        HmpToCsvTaskLarge hmpToCsvTaskLarge = new HmpToCsvTaskLarge(TEST_INPUT_HMP, TEST_OUTPUT_HMP, START_LINE_HMP, END_LINE_HMP, majorAlleles, portion);
        assertNotNull(hmpToCsvTaskLarge);
    }

    /**
     * Tests that the main functionality of the worker completes.
     */
    @Test
    @DisplayName("testRun")
    void testRun() {
        final int portion = 1;
        HmpToCsvTaskLarge hmpToCsvTaskLarge = new HmpToCsvTaskLarge(TEST_INPUT_HMP, TEST_OUTPUT_HMP_LARGE, START_LINE_HMP, END_LINE_HMP, majorAlleles, portion);
        try {
            hmpToCsvTaskLarge.run();
        } catch (Exception e) {
            fail("There was a problem during testing in HmpToCsvTaskLargeTest.testRun().");
            //e.printStackTrace();
        }
    }

    /**
     * Tests whether the HmpToCsvTaskLarge worker accurately increments its file counter.
     */
    @Test
    @DisplayName("testGetNumberOfFilesInSeries")
    void testGetNumberOfFilesInSeries() {
        final int portion = 1;
        HmpToCsvTaskLarge hmpToCsvTaskLarge = new HmpToCsvTaskLarge(TEST_INPUT_HMP, TEST_OUTPUT_HMP_LARGE, START_LINE_HMP, END_LINE_HMP, majorAlleles, portion);
        
        assertEquals(0, hmpToCsvTaskLarge.getNumberOfFilesInSeries());
        hmpToCsvTaskLarge.run();
        assertEquals(1, hmpToCsvTaskLarge.getNumberOfFilesInSeries()); // As long as chunkSize is 512~ and test files remain the same size.
    }

    /**
     * Tests whether the HmpToCsvTaskLarge worker accurately transposes and translates
     * its provided data.
     */
    @Test
    @DisplayName("testHmpToCsvTransposeAccuracy")
    void testHmpToCsvTransposeAccuracy() {

        final String TEST_HMP_TRANSPOSE_IN = "./src/test/resources/testHmpToCsvTranspose.hmp";
        final String TEST_HMP_TRANSPOSE_OUT = "./src/test/resources/testHmpToCsvTranspose.csv";
        final String TEST_HMP_TRANSPOSE_OUT_FINAL = "./src/test/resources/temp0/testHmpToCsvTranspose.csv0";

        final String[] alleles = new String[]{"A", "C", "T", "G", "N"};

        final int portion = 0;
        HmpToCsvTaskLarge hmpToCsvTaskLarge = new HmpToCsvTaskLarge(TEST_HMP_TRANSPOSE_IN, TEST_HMP_TRANSPOSE_OUT, START_LINE_HMP, END_LINE_HMP, alleles, portion);
        hmpToCsvTaskLarge.run();

        // Prepend the phenotype placeholder
        String[] expectedResults = new String[] {
            "-9,0,0,0,0,0",
            "-9,2,2,2,2,2",
            "-9,2,0,2,2,2",
            "-9,2,2,2,2,2",
            "-9,0,0,2,2,2",
            "-9,2,0,0,2,2",
            "-9,1,1,0,2,5",
            "-9,0,0,0,2,2",
            "-9,2,0,2,2,2",
            "-9,2,2,0,2,2"
        };

        // Read in output file compare it to expected results:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(TEST_HMP_TRANSPOSE_OUT_FINAL));
        ) {
            for (int i = 0; i < 10; ++i) {
                String line = reader.readLine();
                //System.out.println("Read:[" + line + "]");
                //System.out.println("Exp.:[" + expectedResults[i] + "]");
                assertEquals(expectedResults[i], line);
            }
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testHmpToCsvTransposeAccuracy - FileNotFoundException.");
        } catch (IOException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testHmpToCsvTransposeAccuracy - IOException.");
        }
    }

}
