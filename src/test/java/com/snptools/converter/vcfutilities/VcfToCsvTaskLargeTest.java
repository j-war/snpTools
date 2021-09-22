package com.snptools.converter.vcfutilities;

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
 * A set of unit tests for VcfToCsvTaskLargeTest.
 * 
 * @author  Jeff Warner
 * @version 1.0, September 2021
 */
public class VcfToCsvTaskLargeTest {

    final String TEST_INPUT_VCF = "./src/test/resources/testFullNormal.vcf";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testVcf.csv";
    final String TEST_OUTPUT_VCF_LARGE = "./src/test/resources/LARGEtestVcf.csv"; // Does not exist - placeholder value.

    final int START_LINE_VCF = 0;
    final int END_LINE_VCF = 5;

    final int TOTAL_LINES = END_LINE_VCF - START_LINE_VCF;

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.

    /**
     * Tests whether the VcfToCsvTaskLarge worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructVcfToCsvTaskLarge")
    public void shouldConstructVcfToCsvTaskLarge() {
        final int portion = 1;
        VcfToCsvTaskLarge vcfToCsvTaskLarge = new VcfToCsvTaskLarge(TEST_INPUT_VCF, TEST_OUTPUT_VCF, START_LINE_VCF, END_LINE_VCF, portion);
        assertNotNull(vcfToCsvTaskLarge);
    }

    /**
     * Tests that the main functionality of the worker completes.
     */
    @Test
    @DisplayName("testRun")
    void testRun() {
        final int portion = 1;
        VcfToCsvTaskLarge vcfToCsvTaskLarge = new VcfToCsvTaskLarge(TEST_INPUT_VCF, TEST_OUTPUT_VCF_LARGE, START_LINE_VCF, END_LINE_VCF, portion);
        try {
            vcfToCsvTaskLarge.run();
        } catch (Exception e) {
            //e.printStackTrace();
            fail("There was a problem during testing in VcfToCsvTaskLargeTest.testRun().");
        }
    }

    /**
     * Tests whether the VcfToCsvTaskLarge worker accurately increments its file counter.
     */
    @Test
    @DisplayName("testGetNumberOfFilesInSeries")
    void testGetNumberOfFilesInSeries() {
        final int portion = 1;
        VcfToCsvTaskLarge vcfToCsvTaskLarge = new VcfToCsvTaskLarge(TEST_INPUT_VCF, TEST_OUTPUT_VCF_LARGE, START_LINE_VCF, END_LINE_VCF, portion);
        
        assertEquals(0, vcfToCsvTaskLarge.getNumberOfFilesInSeries());
        vcfToCsvTaskLarge.run();
        assertEquals(1, vcfToCsvTaskLarge.getNumberOfFilesInSeries()); // As long as chunkSize is 512~ and test files remain the same size.
    }

    /**
     * Tests whether the VcfToCsvTaskLarge worker accurately transposes and translates
     * its provided data.
     */
    @Test
    @DisplayName("testVcfToCsvTransposeAccuracy")
    void testVcfToCsvTransposeAccuracy() {

        final String TEST_VCF_TRANSPOSE_IN = "./src/test/resources/testVcfToCsvTranspose.vcf";
        final String TEST_VCF_TRANSPOSE_OUT = "./src/test/resources/testVcfToCsvTranspose.csv";
        final String TEST_VCF_TRANSPOSE_OUT_FINAL = "./src/test/resources/temp1/testVcfToCsvTranspose.csv0";

        final String[] alleles = new String[]{"A", "C", "T", "G", "N"};

        final int portion = 1;
        VcfToCsvTaskLarge vcfToCsvTaskLarge = new VcfToCsvTaskLarge(TEST_VCF_TRANSPOSE_IN, TEST_VCF_TRANSPOSE_OUT, START_LINE_VCF, END_LINE_VCF, portion);
        vcfToCsvTaskLarge.run();

        String[] expectedResults = new String[] {
            "0,1,2,1,5",
            "5,2,2,2,2",
            "2,2,2,2,2",
            "2,2,2,2,2",
            "2,0,2,2,2",
            "2,2,1,2,2",
            "2,1,2,5,9",
            "2,2,2,2,0",
            "2,2,2,1,2",
            "2,1,1,5,1"
        };

        // Read in output file compare it to expected results:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(TEST_VCF_TRANSPOSE_OUT_FINAL));
        ) {
            for (int i = 0; i < 10; ++i) {
                String line = reader.readLine();
                //System.out.println("Read:[" + line + "]");
                //System.out.println("Exp.:[" + expectedResults[i] + "]");
                assertEquals(expectedResults[i], line);
            }
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testVCFToCsvTransposeAccuracy - FileNotFoundException.");
        } catch (IOException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testVcfToCsvTransposeAccuracy - IOException.");
        }
    }

}
