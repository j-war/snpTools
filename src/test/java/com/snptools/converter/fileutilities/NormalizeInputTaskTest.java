package com.snptools.converter.fileutilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for NormalizeInputTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class NormalizeInputTaskTest {

    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";

    final String TEST_INPUT_VCF = "./src/test/resources/test.vcf";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testVcf.csv";

    final String TEST_INPUT_HMP = "./src/test/resources/test.hmo";
    final String TEST_OUTPUT_HMP = "./src/test/resources/testHmp.csv";

    final int START_LINE_PED = 0;
    final int END_LINE_PED = 3;

    final int START_LINE_VCF = 11;
    final int END_LINE_VCF = 16;

    final int START_LINE_HMP = 1;
    final int END_LINE_HMP = 6;

    final int START_COLUMN_PED = 6; // Where this worker should start.
    final int NUMBER_OF_COLUMNS_PED = 10; // The number of columns that should be kept by this worker.

    final int START_COLUMN_VCF = 6; // Where this worker should start - how many to skip.
    final int NUMBER_OF_COLUMNS_VCF = 10; // The number of columns that should be kept by this worker.

    final int START_COLUMN_HMP = 11; // Where this worker should start - how many to skip.
    final int NUMBER_OF_COLUMNS_HMP = 10; // The number of columns that should be kept by this worker.

    final int COLUMN_WIDTH_VCF = 3; // <--- Depends on ploidiness. Diploid=3 in VCF.
    final int COLUMN_WIDTH_HMP = 2; // <--- Depends on ploidiness. Diploid=2 in HMP.

    /**
     * Tests whether the NormalizeInputTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructNormalizeInputTask")
    public void shouldConstructNormalizeInputTask() {
        NormalizeInputTask normalizeInputTask = new NormalizeInputTask(TEST_INPUT_PED, TEST_OUTPUT_PED, START_LINE_PED, END_LINE_PED, START_COLUMN_PED, NUMBER_OF_COLUMNS_PED, COLUMN_WIDTH_VCF);
        assertNotNull(normalizeInputTask);
    }

    /**
     * For Vcf file normalization:
     * 
     * Tests whether the NormalizeInputTask worker properly truncates
     * and stores data in partialResults.
     */
    @Test
    @DisplayName("shouldAccumulateVcfResults")
    public void shouldAccumulateVcfResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        // For VCF:
        // vcf: startColumn = number of headers to skip, numberOfColumns = number of columns to keep, columnWidth = 3
        // Create a test string to pass to accumulate()
        // create an expectedPartialResults array
        //
        // Repeat with a good and a bad string:
        // Create a Normalize worker
        // call accumulate with test string
        // ?copy its partialResults array?
        // Compare the partial results to expected results in a loop.

        // Construct two arrays: one containing the expected values - entries need to be the same length as columnWidth, and another with "bad" results:
        final String testVcfInputOne = "skip\t123\t13:skip\t[0.1}skip\t0/1\tNA\tTest:gh834t9hh\tbye123\tKye456\tb,ye78\t11bye\t0/2bye:[1.2;]\tbye\tH.ey\t./.:[2.4]\tLastbye";
        final String[] expectedVcfResultsOne = new String[]{"Tes", "bye", "Kye", "b,y", "11b", "0/2", "bye", "H.e", "./.", "Las"};
        final String testVcfInputTwo = "1\t0\t13:skip\t[0.1}skip\t0/1\tNA\tTest:gh834t9hh\tbye123\tKye456\tb,ye78\t11bye\t0/2bye:[1.2;]\tbye\tH.ey\t./.:[2.4]\tLastbye";
        final String[] expectedVcfResultsTwo = new String[]{"Not", "the", "exp", "ect", "edR", "esu", "lts", "1/1", "2/2", "End"};


        NormalizeInputTask normalizeFromVcfTask =
        new NormalizeInputTask(
            TEST_INPUT_VCF, TEST_OUTPUT_VCF,
            START_LINE_VCF, END_LINE_VCF,
            START_COLUMN_VCF, NUMBER_OF_COLUMNS_VCF,
            COLUMN_WIDTH_VCF
        );

        Method accumulateVcfResults = NormalizeInputTask.class.getDeclaredMethod("accumulateResults", String.class);
        accumulateVcfResults.setAccessible(true);

        // Invoke with a "good" string:
        accumulateVcfResults.invoke(normalizeFromVcfTask, testVcfInputOne);

        Field partialVcfResults = normalizeFromVcfTask.getClass().getDeclaredField("partialResults");
        partialVcfResults.setAccessible(true);
        String[] vcfTestResults = (String[]) partialVcfResults.get(normalizeFromVcfTask);

        // Check that results were obtained
        // Check that the correct number were obtained
        // Check that the entries in the array are the correct length AND match expected values:
        Assertions.assertAll(
            () -> assertTrue(vcfTestResults.length > 0),
            () -> assertTrue(expectedVcfResultsOne.length > 0),
            () -> assertEquals(expectedVcfResultsOne.length, vcfTestResults.length),
            () -> {
                for (int i = 0; i < NUMBER_OF_COLUMNS_VCF; ++i) {
                    assertEquals(expectedVcfResultsOne[i], vcfTestResults[i]);
                    assertTrue(vcfTestResults[i].equalsIgnoreCase(expectedVcfResultsOne[i]));
                    assertEquals(vcfTestResults[i].length(), COLUMN_WIDTH_VCF);
                }
            }
        );


        // Invoke with a "bad" string:
        accumulateVcfResults.invoke(normalizeFromVcfTask, testVcfInputTwo);

        Field partialVcfResultsTwo = normalizeFromVcfTask.getClass().getDeclaredField("partialResults");
        partialVcfResultsTwo.setAccessible(true);
        String[] vcfTestResultsTwo = (String[]) partialVcfResultsTwo.get(normalizeFromVcfTask);

        // Check that results were obtained
        // Check that the correct number were obtained
        // Check that the entries do not match bad values but are the expected length:
        Assertions.assertAll(
            () -> assertTrue(vcfTestResultsTwo.length > 0),
            () -> assertTrue(expectedVcfResultsTwo.length > 0),
            () -> assertEquals(expectedVcfResultsTwo.length, vcfTestResultsTwo.length),
            () -> {
                for (int i = 0; i < NUMBER_OF_COLUMNS_VCF; ++i) {
                    assertFalse(expectedVcfResultsTwo[i] == vcfTestResultsTwo[i]);
                    assertFalse(vcfTestResultsTwo[i].equalsIgnoreCase(expectedVcfResultsTwo[i]));
                    assertEquals(vcfTestResultsTwo[i].length(), COLUMN_WIDTH_VCF);
                }
            }
        );
    }

    /**
     * For HMP file normalization:
     * 
     * Tests whether the NormalizeInputTask worker properly truncates
     * and stores data in partialResults.
     */
    @Test
    @DisplayName("shouldAccumulateHmpResults")
    public void shouldAccumulateHmpResults() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        // For HMP:
        // hmp: startColumn = number of headers to skip, numberOfColumns = number of columns to keep, columnWidth = 2
        // Create a test string to pass to accumulate()
        // create an expectedPartialResults array
        //
        // Repeat with a good and bad string:
        // Create a Normalize worker
        // call accumulate with test string
        // copy its partialResults array
        // Compare the partial results to expected results in a loop.

        // Construct two arrays: one containing the expected values - entries need to be the same length as columnWidth, and another with "bad" results:
        final String testHmpInputOne =
        "98234.1\tA/C\t1\t2465\t+\tskip\t123\t13:skip\t[0.1}skip\t0/1\tNA\tTest:gh834t9hh\tbye123\tKye456\tb,ye78\t11bye\tAA2bye:[1.2;]\tCCe\tH.ey\t./.:[2.4]\tLastbye";
        final String[] expectedHmpResultsOne = new String[]{"Te", "by", "Ky", "b,", "11", "AA", "CC", "H.", "./", "La"};

        final String testHmpInputTwo = "34.\tAC\t0\t29845689456\t-\tskip23\t123\t13:skip\t[0.1}skip\t0/1\tNA\tTest:gh834t9hh\tbye123\tKye456\tb,ye78\t11bye\t0/2bye:[1.2;]\tbye\tH.ey\t./.:[2.4]\tLastbye";
        final String[] expectedHmpResultsTwo = new String[]{"Not", "the", "exp", "ect", "edR", "esu", "lts", "1/1", "2/2", "End"};


        NormalizeInputTask normalizeFromHmpTask =
        new NormalizeInputTask(
            TEST_INPUT_HMP, TEST_OUTPUT_HMP,
            START_LINE_HMP, END_LINE_HMP,
            START_COLUMN_HMP, NUMBER_OF_COLUMNS_HMP,
            COLUMN_WIDTH_HMP
        );

        Method accumulateHmpResults = NormalizeInputTask.class.getDeclaredMethod("accumulateResults", String.class);
        accumulateHmpResults.setAccessible(true);

        // Invoke with a "good" string:
        accumulateHmpResults.invoke(normalizeFromHmpTask, testHmpInputOne);

        Field partialHmpResults = normalizeFromHmpTask.getClass().getDeclaredField("partialResults");
        partialHmpResults.setAccessible(true);
        String[] hmpTestResultsOne = (String[]) partialHmpResults.get(normalizeFromHmpTask);

        // Check that results were obtained
        // Check that the correct number were obtained
        // Check that the entries in the array are the correct length AND match expected values:
        Assertions.assertAll(
            () -> assertTrue(hmpTestResultsOne.length > 0),
            () -> assertTrue(expectedHmpResultsOne.length > 0),
            () -> assertEquals(expectedHmpResultsOne.length, hmpTestResultsOne.length),
            () -> {
                for (int i = 0; i < NUMBER_OF_COLUMNS_HMP; ++i) {
                    assertEquals(expectedHmpResultsOne[i], hmpTestResultsOne[i]);
                    assertTrue(hmpTestResultsOne[i].equalsIgnoreCase(expectedHmpResultsOne[i]));
                    assertEquals(hmpTestResultsOne[i].length(), COLUMN_WIDTH_HMP);
                }
            }
        );

        // Invoke with a "bad" string:
        accumulateHmpResults.invoke(normalizeFromHmpTask, testHmpInputTwo);

        Field partialHmpResultsTwo = normalizeFromHmpTask.getClass().getDeclaredField("partialResults");
        partialHmpResultsTwo.setAccessible(true);
        String[] hmpTestResultsTwo = (String[]) partialHmpResultsTwo.get(normalizeFromHmpTask);

        // Check that results were obtained
        // Check that the correct number were obtained
        // Check that the entries do not match bad values but are the expected length:
        Assertions.assertAll(
            () -> assertTrue(hmpTestResultsTwo.length > 0),
            () -> assertTrue(expectedHmpResultsTwo.length > 0),
            () -> assertEquals(expectedHmpResultsTwo.length, hmpTestResultsTwo.length),
            () -> {
                for (int i = 0; i < NUMBER_OF_COLUMNS_HMP; ++i) {
                    assertFalse(expectedHmpResultsTwo[i] == hmpTestResultsTwo[i]);
                    assertFalse(hmpTestResultsTwo[i].equalsIgnoreCase(expectedHmpResultsTwo[i]));
                    assertEquals(hmpTestResultsTwo[i].length(), COLUMN_WIDTH_HMP);
                }
            }
        );
    }

}
