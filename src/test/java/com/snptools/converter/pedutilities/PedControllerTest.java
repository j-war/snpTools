package com.snptools.converter.pedutilities;

import com.snptools.converter.fileutilities.FileController;
import java.io.File;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * A set of unit tests for PedController.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class PedControllerTest {

    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";
    final int START_LINE = 0;
    final int END_LINE = 3;
    final int COLUMN_PAIRS = 474; // is equal to half the number of data columns in the test file.

    /**
     * Tests whether the controller is constructed with the correct 
     * input and output file properties set.
     */
    @Test
    @DisplayName("shouldConstructPedController")
    public void shouldConstructPedController() {
        PedController pedController = new PedController(TEST_INPUT_PED, TEST_OUTPUT_PED);
        Assertions.assertAll(
            () -> assertNotNull(pedController),
            () -> assertEquals(TEST_INPUT_PED, pedController.getInputFilePath()),
            () -> assertEquals(TEST_OUTPUT_PED, pedController.getOutputFilePath())
        );
    }

    /**
     * Tests whether the testing .ped file is present.
     */
    @Test
    @DisplayName("readTestInputPedFile")
    public void readTestInputPedFile() {
        File file = new File(TEST_INPUT_PED);
        assertTrue(file.exists());
    }

    /**
     * Tests whether the testing output directory exists.
     */
    @Test
    @DisplayName("readTestOutputDir")
    public void readTestOutputDir() {
        // Assert output path is not null, blank, or empty.
        Assertions.assertAll(
            () -> assertNotNull(TEST_OUTPUT_PED),
            () -> assertTrue( !(TEST_OUTPUT_PED.isBlank()) ),
            () -> assertTrue( !(TEST_OUTPUT_PED.isEmpty()) )
        );
        File folder = new File(new File(TEST_OUTPUT_PED).getParent());
        Assertions.assertAll(
            () -> assertNotNull(folder),
            () -> assertTrue(folder.exists()),
            () -> assertTrue(folder.isDirectory())
        );
    }

    /**
     * Tests whether the counting of the total lines is correct - ped files do not have
     * file headers but do have line headers on every line.
     */
    @Test
    @DisplayName("testCountPedTotalLines")
    public void testCountPedTotalLines() {
        assertEquals((END_LINE - START_LINE), FileController.countTotalLines(TEST_INPUT_PED)); 
    }

    /**
     * Tests whether the number of data columns is correct. The input data should be collected
     * in pairs, therefore the output will contain half as many as there are in the input.
     */
    @Test
    @DisplayName("testCountPedPairLineLength")
    public void testCountPedPairLineLength() {
        PedController pedController = new PedController(TEST_INPUT_PED, TEST_OUTPUT_PED);
        assertEquals(COLUMN_PAIRS, pedController.countLineLength());
    }

    /**
     * Tests whether the totals datastructure was initialized correctly.
     * 
     * This test uses reflection to selectively call private methods.
     */
    @Test
    @DisplayName("testTotalsInitialized")
    public void testTotalsInitialized() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException, NoSuchMethodException, SecurityException {
        PedController pedController = new PedController(TEST_INPUT_PED, TEST_OUTPUT_PED);
        final int SIZE_OF_LIST = 105; // 105 is arbitrary.

        Method initTotalsMethod = PedController.class.getDeclaredMethod("initTotals");
        initTotalsMethod.setAccessible(true);
        Field inputColumnCount = pedController.getClass().getDeclaredField("inputColumnCount");
        inputColumnCount.setAccessible(true);

        inputColumnCount.set(pedController, SIZE_OF_LIST);
        initTotalsMethod.invoke(pedController);

        assertEquals(SIZE_OF_LIST, (pedController.getTotals()).size());
    }

    /**
     * Tests whether the majorAlleles datastructure was initialized correctly by
     * summing the test input file and calling calculateMajors() through reflection.
     * 
     * This test uses reflection to selectively call private methods and to set
     * private fields for convenience. Some select allele pairs are checked that
     * may be problematic due to ambiguities - example: missing data ("0"), equal
     * number of pairs in a column (should take the lexigraphical ordering according
     * to the pre-filled dictionary.)
     */
    @Test
    @DisplayName("testSummingAndThenCalculateMajors")
    public void testSummingAndThenCalculateMajors() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException, NoSuchMethodException, SecurityException, InterruptedException {
        // 1. set workers to = 1
        // 2. set totalInputLines = 3
        // 3. set inputColumnCount = 474
        // 4. should call processInputThreaded but instead just call run/start on pedSumTask
        // 5. initTotals()
        // 6. Several options:
        //      x-> mergeThreadTotals() 
        //       -> set the controllers field = tasks field. <-
        //      x-> processInputThreaded(1)
        // 7. >>>> calculateMajors()
        // 8. >>>> calculateResultsThreaded
        //      -> Cherry pick a few points.

        // 1.
        PedController pedController = new PedController(TEST_INPUT_PED, TEST_OUTPUT_PED);
        Field NUMBER_OF_WORKERS = pedController.getClass().getDeclaredField("NUMBER_OF_WORKERS");
        NUMBER_OF_WORKERS.setAccessible(true);
        NUMBER_OF_WORKERS.set(pedController, 1); // set to 1 for single thread.

        // 2.
        Field totalInputLines = pedController.getClass().getDeclaredField("totalInputLines");
        totalInputLines.setAccessible(true);
        totalInputLines.set(pedController, (END_LINE - START_LINE));

        // 3.
        Field inputColumnCount = pedController.getClass().getDeclaredField("inputColumnCount");
        inputColumnCount.setAccessible(true);
        inputColumnCount.set(pedController, COLUMN_PAIRS);

        // 4. Short form of processInputThreaded().
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS);
        Thread task = new Thread(pedSumTask);
        task.start();
        task.join();

        // 5.
        Method initTotalsMethod = PedController.class.getDeclaredMethod("initTotals");
        initTotalsMethod.setAccessible(true);
        initTotalsMethod.invoke(pedController);

        // 6.
        /*Method mergeTotalsMethod = PedController.class.getDeclaredMethod("mergeThreadTotals");
        mergeTotalsMethod.setAccessible(true);
        mergeTotalsMethod.invoke(pedController);*/
        // 6. alt. set pedController.totals = pedsumtask.totals
        Field totals = pedController.getClass().getDeclaredField("totals");
        totals.setAccessible(true);
        totals.set(pedController, pedSumTask.getTotals());
        // 6. alt 2. call processInputThreaded(1).
        /*Method processInputThreaded = PedController.class.getDeclaredMethod("processInputThreaded", int.class);
        processInputThreaded.setAccessible(true);
        processInputThreaded.invoke(pedController, 1);*/

        // 7.
        Method calculateMajorsMethod = PedController.class.getDeclaredMethod("calculateMajors");
        calculateMajorsMethod.setAccessible(true);
        calculateMajorsMethod.invoke(pedController);

        // 8.
        // Compare expected and actual
        // Cherry picked data points:
        // 229 - 1 contains: 0-0, C-C, T-T
        // 246 - 1 contains: G-G, C-C, 0-0
        Assertions.assertAll(
            () ->
            assertEquals(COLUMN_PAIRS, pedController.majorAllelesValues.length),
            () -> assertEquals("C", pedController.majorAllelesValues[0]),
            () -> assertEquals("C", pedController.majorAllelesValues[1]),
            () -> assertEquals("G", pedController.majorAllelesValues[2]),
            () -> assertEquals("T", pedController.majorAllelesValues[3]),
            () -> assertEquals("G", pedController.majorAllelesValues[4]),
            () -> assertEquals("C", pedController.majorAllelesValues[229 - 1]),
            () -> assertEquals("C", pedController.majorAllelesValues[246 - 1]),
            () -> assertEquals("A", pedController.majorAllelesValues[246]),
            () -> assertEquals("T", pedController.majorAllelesValues[COLUMN_PAIRS - 1])
        );
    }

}
