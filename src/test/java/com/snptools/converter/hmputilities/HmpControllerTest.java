package com.snptools.converter.hmputilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for HmpController.
 * 
 * @author  Jeff Warner
 * @version 1.0, September 2021
 */
public class HmpControllerTest {

    final String TEST_INPUT_HMP = "./src/test/resources/test.hmp";
    final String TEST_OUTPUT_HMP = "./src/test/resources/testHmp.csv";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testHmp.vcf";
    final String TEST_VCF_HEADERS = "./src/test/resources/vcfHeaders.vcf";
    final int START_LINE_HMP = 11;
    final int END_LINE_HMP = 16;

    final int TOTAL_LINES = END_LINE_HMP - START_LINE_HMP;

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
        HmpController hmpController = new HmpController(TEST_INPUT_HMP, TEST_OUTPUT_HMP);
        Assertions.assertAll(
            () -> assertNotNull(hmpController),
            () -> assertEquals(TEST_INPUT_HMP, hmpController.getInputFilePath()),
            () -> assertEquals(TEST_OUTPUT_HMP, hmpController.getOutputFilePath())
        );
    }

    // csv and vcf
    // x countTotalLines() (Tested in FileControllerTest)
    // x countLineLength()
    // x determinePloidiness()
    // both normalize(), vcf merges though.
    // processInputThreaded() -> sumtask (Tested in HmpSumTaskTest)
    // initTotals()
    // mergeThreadTotals()
    // calculateMajors()

    // csv only:
    // convertHmpToCsv() -> convertHmpToCsvLargeThreaded()

    // vcf only:
    // makeVcfHeaders():
    //      initLineHeaders()
    //      collectLineHeaders()
    //      collectSampleIds()
    //      collectStrandDirections()
    //      collectAllAlleles()
    //      createOutputLineHeaders()
    // convertHmpToVcf() -> convertHmpToVcfThreaded()


    /**
     * Tests whether the controller accurately counts the
     * lines of hmp files. Skips the header line and counts all
     * columns.
     * 
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("testCountLineLength")
    void testCountLineLength() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        HmpController hmpController = new HmpController(TEST_INPUT_HMP, TEST_OUTPUT_HMP);

        Method countLineLengthMethod = HmpController.class.getDeclaredMethod("countLineLength");
        countLineLengthMethod.setAccessible(true);

        assertEquals(292, countLineLengthMethod.invoke(hmpController));
    }

    /**
     * Tests whether the controller accurately determines the ploidiness
     * of the hmp input file.
     * 
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("testDeterminePloidiness")
    void testDeterminePloidiness() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        HmpController hmpController = new HmpController(TEST_INPUT_HMP, TEST_OUTPUT_HMP);

        Method determinePloidinessMethod = HmpController.class.getDeclaredMethod("determinePloidiness");
        determinePloidinessMethod.setAccessible(true);

        assertEquals(2, determinePloidinessMethod.invoke(hmpController));
    }

    /**
     * Tests whether the controller properly creates the vcf headers.
     */
    @Test
    @DisplayName("testMakeVcfHeaders")
    void testMakeVcfHeaders() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {

        HmpController hmpController = new HmpController(TEST_INPUT_HMP, TEST_OUTPUT_VCF);

        Field totalInputLines = hmpController.getClass().getDeclaredField("totalInputLines");
        totalInputLines.setAccessible(true);
        totalInputLines.set(hmpController, 6);
        int totalInputLinesResults = (int) totalInputLines.get(hmpController);
        assertEquals(6, totalInputLinesResults);


        Field totalInputColumns = hmpController.getClass().getDeclaredField("totalInputColumns");
        totalInputColumns.setAccessible(true);
        totalInputColumns.set(hmpController, 292);
        int totalInputColumnsResults = (int) totalInputColumns.get(hmpController);
        assertEquals(292, totalInputColumnsResults);


        Method determinePloidinessMethod = HmpController.class.getDeclaredMethod("determinePloidiness");
        determinePloidinessMethod.setAccessible(true);
        int ploidValue = (int) determinePloidinessMethod.invoke(hmpController);
        Field hmpPloidiness = hmpController.getClass().getDeclaredField("hmpPloidiness");
        hmpPloidiness.setAccessible(true);
        //int hmpPloidinessResults = (int) hmpPloidiness.get(hmpController);
        assertEquals(2, ploidValue);


        Method initTotalsMethod = HmpController.class.getDeclaredMethod("initTotals");
        initTotalsMethod.setAccessible(true);
        initTotalsMethod.invoke(hmpController);
        Field totals = hmpController.getClass().getDeclaredField("totals");
        totals.setAccessible(true);


        List<int[]> totalsResults = hmpController.getTotals(); // (List<int[]>) totals.get(hmpController);
        totalsResults.get(0)[0] = 10; // a
        totalsResults.get(0)[1] = 8; // c

        totalsResults.get(1)[1] = 17; // c
        totalsResults.get(1)[2] = 1; // g

        totalsResults.get(2)[2] = 10; // g
        totalsResults.get(2)[3] = 8; // t

        totalsResults.get(3)[0] = 10; // a
        totalsResults.get(3)[1] = 8; // c

        totalsResults.get(4)[1] = 10; // c
        totalsResults.get(4)[2] = 8; // g

        assertEquals(5, totalsResults.size());


        //mergeThreadTotals(); // not needed in test.

        Method calculateMajorsMethod = HmpController.class.getDeclaredMethod("calculateMajors");
        calculateMajorsMethod.setAccessible(true);
        calculateMajorsMethod.invoke(hmpController);
        Field majorAllelesValues = hmpController.getClass().getDeclaredField("majorAllelesValues");
        majorAllelesValues.setAccessible(true);
        String[] majorAllelesResults = (String[]) majorAllelesValues.get(hmpController);
        assertEquals(5, majorAllelesResults.length);


        Method initLineHeadersMethod = HmpController.class.getDeclaredMethod("initLineHeaders");
        initLineHeadersMethod.setAccessible(true);
        initLineHeadersMethod.invoke(hmpController);


        Method collectLineHeadersMethod = HmpController.class.getDeclaredMethod("collectLineHeaders");
        collectLineHeadersMethod.setAccessible(true);
        collectLineHeadersMethod.invoke(hmpController);


        Method collectSampleIdsMethod = HmpController.class.getDeclaredMethod("collectSampleIds");
        collectSampleIdsMethod.setAccessible(true);
        collectSampleIdsMethod.invoke(hmpController);


        Method collectStrandDirectionsMethod = HmpController.class.getDeclaredMethod("collectStrandDirections");
        collectStrandDirectionsMethod.setAccessible(true);
        collectStrandDirectionsMethod.invoke(hmpController);


        Method collectAllAllelesMethod = HmpController.class.getDeclaredMethod("collectAllAlleles");
        collectAllAllelesMethod.setAccessible(true);
        collectAllAllelesMethod.invoke(hmpController);

        Method createOutputLineHeadersMethod = HmpController.class.getDeclaredMethod("createOutputLineHeaders");
        createOutputLineHeadersMethod.setAccessible(true);
        createOutputLineHeadersMethod.invoke(hmpController);


        Field outputLineHeaders = hmpController.getClass().getDeclaredField("outputLineHeaders");
        outputLineHeaders.setAccessible(true);
        String[] outputLineHeadersResults = (String[]) outputLineHeaders.get(hmpController);

        assertEquals(5, outputLineHeadersResults.length);
        assertEquals(1727, outputLineHeadersResults[0].length());

        for (int i = 0; i < 5; ++i) {
            //System.out.println("[" + outputLineHeadersResults[i] + "]");
        }

        // Read in output file compare it to expected results:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(TEST_VCF_HEADERS));
        ) {
            String line = "";
            for (int i = 0; i < 4; ++i) {
                reader.readLine(); // Skip ahead.
            }
            for (int i = 1; i < 5; ++i) { // Only check the data lines, excluding entry 1 since it contains file header info too.
                line = reader.readLine();

                String[] readerEntries = line.split("\\s+");
                String[] loadedEntries = outputLineHeadersResults[i].split("\\s+");


                for (int j = 0; j < readerEntries.length; ++j) {
                    assertTrue(readerEntries.length > 0);
                    assertTrue(readerEntries[i].length() > 0);
                    assertTrue(readerEntries[j].length() > 0);
                    assertTrue(loadedEntries.length > 0);
                    assertTrue(loadedEntries[i].length() > 0);
                    assertTrue(loadedEntries[j].length() > 0);
                    assertTrue(loadedEntries[j].length() == readerEntries[j].length());
                    assertTrue(loadedEntries.length == readerEntries.length);
                    //System.out.println("Checking:" + i + " | " + j);
                    //System.out.println("[" + readerEntries[j] + "] | [" + loadedEntries[j] + "]\n");
                    assertTrue(loadedEntries[j].equalsIgnoreCase(readerEntries[j]));
                }

            }
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testMakeVcfHeaders - FileNotFoundException.");
        } catch (IOException e) {
            //e.printStackTrace();
            fail("There was a problem during testing in testMakeVcfHeaders - IOException.");
        }
    
    }

}
