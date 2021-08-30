package com.snptools.converter.hmputilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for HmpSumTask. The input test lines should be csv style strings
 * as HmpSumTask expects data from a NormalizedTask worker.
 * 
 * @author  Jeff Warner
 * @version 1.1, August 2021
 */
public class HmpSumTaskTest {

    final String TEST_INPUT_HMP = "./src/test/resources/testHalfNormal.hmp";
    final String TEST_OUTPUT_HMP = "./src/test/resources/testHmp.csv";

    final int START_LINE_HMP = 1;
    final int END_LINE_HMP = 6;

    final int TOTAL_LINES = END_LINE_HMP - START_LINE_HMP;
    final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.

    final int NUMBER_OF_BASES_TO_SUM = 2 * (NUMBER_OF_COLUMNS);
    final int NUMBER_OF_BASES = 16 + 1; // ACTG...

    /**
     * Tests whether the HmpSumTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructHmpSumTask")
    public void shouldConstructHmpSumTask() {
        HmpSumTask hmpSumTask = new HmpSumTask(TEST_INPUT_HMP, START_LINE_HMP, END_LINE_HMP, COLUMN_PAIRS, false);
        assertNotNull(hmpSumTask);
    }

    /**
     * Tests whether the totals datastructure was initialized and
     * filled correctly by checking that the sums are consistent.
     */
    @Test
    @DisplayName("testInitAndCollectTotals")
    void testInitAndCollectTotals() throws InterruptedException {
        HmpSumTask hmpSumTask = new HmpSumTask(TEST_INPUT_HMP, START_LINE_HMP, END_LINE_HMP, NUMBER_OF_COLUMNS, false);
        Thread task = new Thread(hmpSumTask);
        task.start();
        task.join();

        Assertions.assertAll(
            () -> assertTrue((hmpSumTask.getTotals()).size() > 0),
            () -> assertEquals(TOTAL_LINES, (hmpSumTask.getTotals()).size()),
            () -> {
                for (int i = 0; i < END_LINE_HMP - START_LINE_HMP; ++i) {
                    int sum = 0;
                    for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                        sum += (hmpSumTask.getTotals()).get(i)[j];
                    }
                    assertEquals(NUMBER_OF_BASES_TO_SUM, sum);
                }
            }
        );
    }

    /**
     * Tests whether a read line from the input file is correctly parsed and
     * interpreted into the totals data structure from the HmpSumTask workers.
     *
     * Note: This test uses reflection to selectively call private methods.
     */
    @Test
    @DisplayName("shouldAccumulateTotals")
    public void shouldAccumulateTotals() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        // 1. Prepare test data and expectedResults
        //      - Test data should look like hmp normalized form.
        // 2. Create a HmpSumTask()
        // 3. Call initTotals with reflection
        // 4. Loop on accumulate accumulateTotals() with prepared data
        // 5. Check results: hmpSumTask.getTotals -> compare to expected

        // 1.
        final int NUMBER_OF_TEST_COLUMNS = 10; // The number of HMP entries in the dataLines below.
        final int NUMBER_OF_TESTLINES = 3;
        final int expectedLineSum = 20;
        List<int[]> expectedTotals = Collections.synchronizedList(new ArrayList<>());
        synchronized (expectedTotals) {
            for (int i = 0; i < NUMBER_OF_TESTLINES; ++i) {
                expectedTotals.add(i, new int[NUMBER_OF_BASES]);
            }
        }

        final String dataLineOne = "aa,CC,TT,gG,AC,TG,CT,GT,NN,TT";
        final String dataLineTwo = "NN,GT,GG,TT,AA,TT,Gt,TT,NN,TG";
        final String dataLineThree = "CG,GC,nN,NN,NN,GA,GA,GC,GT,NN";

        // Load the expectedTotals data structure with the hand calculated results:
        // Sum along lines:
        synchronized (expectedTotals) {
            // dataLineOne:
            expectedTotals.get(0)[0] = 3; // A
            expectedTotals.get(0)[1] = 4; // C
            expectedTotals.get(0)[2] = 4; // G
            expectedTotals.get(0)[3] = 7; // T
            expectedTotals.get(0)[14] = 2; // N
            // dataLineTwo:
            expectedTotals.get(1)[0] = 2;
            expectedTotals.get(1)[1] = 0;
            expectedTotals.get(1)[2] = 5;
            expectedTotals.get(1)[3] = 9;
            expectedTotals.get(1)[14] = 4;
            // dataLineThree:
            expectedTotals.get(2)[0] = 2;
            expectedTotals.get(2)[1] = 3;
            expectedTotals.get(2)[2] = 6;
            expectedTotals.get(2)[3] = 1;
            expectedTotals.get(2)[14] = 8;
        }

        // 2.
        HmpSumTask hmpSumTask = new HmpSumTask(TEST_INPUT_HMP, 0, NUMBER_OF_TESTLINES, NUMBER_OF_TEST_COLUMNS, false);

        // 3.
        Method initTotals = HmpSumTask.class.getDeclaredMethod("initTotals");
        initTotals.setAccessible(true);
        initTotals.invoke(hmpSumTask);

        // 4.
        Method accumulateTotalsMethod = HmpSumTask.class.getDeclaredMethod("accumulateTotals", int.class, String.class);
        accumulateTotalsMethod.setAccessible(true);

        accumulateTotalsMethod.invoke(hmpSumTask, 0, dataLineOne);
        accumulateTotalsMethod.invoke(hmpSumTask, 1, dataLineTwo);
        accumulateTotalsMethod.invoke(hmpSumTask, 2, dataLineThree);

        // 5.
        synchronized (expectedTotals) {
            Assertions.assertAll(
                () -> assertTrue((hmpSumTask.getTotals()).size() > 0),
                () -> assertTrue(expectedTotals.size() > 0),
                () -> assertEquals(expectedTotals.size(), (hmpSumTask.getTotals()).size()),
                () -> {
                    for (int i = 0; i < expectedTotals.size(); ++i) {
                        for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                            assertEquals(expectedTotals.get(i)[j], (hmpSumTask.getTotals()).get(i)[j]);
                        }
                    }
                },
                // Check that the correct number of entries were parsed for each dataLine:
                () -> {
                    for (int i = 0; i < NUMBER_OF_TESTLINES; ++i) {
                        int sum = 0;
                        for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                            sum += (hmpSumTask.getTotals()).get(i)[j];
                        }
                        assertEquals(expectedLineSum, sum);
                    }
                }
            );
        }

    }

}
