package com.snptools.converter.vcfutilities;

import com.snptools.converter.fileutilities.FileController;

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
 * A set of unit tests for VcfController.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class VcfControllerTest {

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
     * Tests whether the controller is constructed with the correct 
     * input and output file properties set.
     */
    @Test
    @DisplayName("shouldConstructVcfController")
    public void shouldConstructVcfController() {
        VcfController vcfController = new VcfController(TEST_INPUT_VCF, TEST_OUTPUT_VCF);
        Assertions.assertAll(
            () -> assertNotNull(vcfController),
            () -> assertEquals(TEST_INPUT_VCF, vcfController.getInputFilePath()),
            () -> assertEquals(TEST_OUTPUT_VCF, vcfController.getOutputFilePath())
        );
    }

    /**
     * Tests whether the counting of the total lines is correct - vcf files
     * have a variable length file header as well as line headers for each record.
     */
    @Test
    @DisplayName("testCountVcfTotalLines")
    public void testCountVcfTotalLines() {
        assertEquals(END_LINE_VCF, FileController.countTotalLines(TEST_INPUT_VCF)); 
    }

    /**
     * Tests whether the counting of the header lines is correct - vcf files
     * have a variable length file header as well as line headers for each record.
     * 
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("testCountHeaderLines")
    public void testCountHeaderLines() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
        VcfController vcfController = new VcfController(TEST_INPUT_VCF, TEST_OUTPUT_VCF);

        Method countHeaderLinesMethod = VcfController.class.getDeclaredMethod("countHeaderLines");
        countHeaderLinesMethod.setAccessible(true);

        Field totalInputLines = vcfController.getClass().getDeclaredField("totalInputLines");
        totalInputLines.setAccessible(true);
        totalInputLines.set(vcfController, END_LINE_VCF);

        // Pass in good constraints:
        assertEquals(START_LINE_VCF, countHeaderLinesMethod.invoke(vcfController));

        // Pass in bad constraints to check early exit of the function:
        totalInputLines.set(vcfController, END_LINE_VCF + 1);
        assertTrue((int) countHeaderLinesMethod.invoke(vcfController) != 0);
        assertEquals(START_LINE_VCF, countHeaderLinesMethod.invoke(vcfController));

        // Pass in bad constraints where outer loop completes before #CHROM is detected:
        totalInputLines.set(vcfController, START_LINE_VCF - 1);
        assertTrue((int) countHeaderLinesMethod.invoke(vcfController) == 0);
        assertEquals(0, countHeaderLinesMethod.invoke(vcfController));
    }

    /**
     * Tests whether the counting of the line lengths works
     * as expected.
     * 
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("testCountLineLength")
    public void testCountLineLength() throws NoSuchMethodException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        // Test setup:
        // 1. Set numberOfHeaderLines
        // 2. Call and check rv of countLineLength()
        // 3. Check formatColumnPresent
        // 4. Check numberOfHeaderColumns

        VcfController vcfController = new VcfController(TEST_INPUT_VCF, TEST_OUTPUT_VCF);

        // 1.
        Field numberOfHeaderLines = vcfController.getClass().getDeclaredField("numberOfHeaderLines");
        numberOfHeaderLines.setAccessible(true);
        numberOfHeaderLines.set(vcfController, START_LINE_VCF);

        // 2.
        Method countLineLengthMethod = VcfController.class.getDeclaredMethod("countLineLength");
        countLineLengthMethod.setAccessible(true);
        assertEquals(END_COLUMN, countLineLengthMethod.invoke(vcfController));

        // 3.
        Field formatColumnPresent = vcfController.getClass().getDeclaredField("formatColumnPresent");
        formatColumnPresent.setAccessible(true);
        Boolean testResultsFormat = (Boolean) formatColumnPresent.get(vcfController);
        assertTrue(testResultsFormat);

        // 4.
        Field numberOfHeaderColumns = vcfController.getClass().getDeclaredField("numberOfHeaderColumns");
        numberOfHeaderColumns.setAccessible(true);
        int testResultHeaderColumnCount = (int) numberOfHeaderColumns.get(vcfController);
        assertEquals(START_COLUMN, testResultHeaderColumnCount);
    }

}
