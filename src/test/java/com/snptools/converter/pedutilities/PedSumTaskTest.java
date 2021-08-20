package com.snptools.converter.pedutilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A set of unit tests for PedSumTask.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class PedSumTaskTest {

    // test.ped has 474 * 2 data entries = 948.
    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final int START_LINE = 0;
    final int END_LINE = 3;
    final int COLUMN_PAIRS = 474; // is equal to half the number of data columns.
    final int NUMBER_OF_BASES_TO_SUM = 2 * (END_LINE - START_LINE);
    final int NUMBER_OF_BASES = 5; // ACTG0

    /**
     * Tests whether the PedResultsTask worker was created successfully.
     */
    @Test
    @DisplayName("shouldConstructPedSumTask")
    public void shouldConstructPedSumTask() {
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS);
        assertNotNull(pedSumTask);
    }

    /**
     * Tests whether the totals datastructure was initialized and
     * filled correctly by checking that the sums are consistent.
     */
    @Test
    @DisplayName("testInitAndCollectTotals")
    void testInitAndCollectTotals() throws InterruptedException {
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, START_LINE, END_LINE, COLUMN_PAIRS);
        Thread task = new Thread(pedSumTask);
        task.start();
        task.join();

        Assertions.assertAll(
            () -> assertTrue((pedSumTask.getTotals()).size() > 0),
            () -> assertEquals(COLUMN_PAIRS, (pedSumTask.getTotals()).size()),
            () -> {
                for (int i = 0; i < COLUMN_PAIRS; ++i) {
                    assertEquals(NUMBER_OF_BASES_TO_SUM, (
                        (pedSumTask.getTotals()).get(i)[0]
                        + (pedSumTask.getTotals()).get(i)[1]
                        + (pedSumTask.getTotals()).get(i)[2]
                        + (pedSumTask.getTotals()).get(i)[3]
                        + (pedSumTask.getTotals()).get(i)[4]
                    ));
                }
            }
        );
    }


    /**
     * Tests whether a read line from the input file is correctly parsed and
     * interpreted into the totals data structure from the PedSumTask workers.
     *
     * Note: This test uses reflection to selectively call private methods and to
     *       read private fields.
     */
    @Test
    @DisplayName("shouldAccumulateTotals")
    public void shouldAccumulateTotals() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // 1. Create a testLine that looks like the input file
        //      -> it should be multilined.
        // 2. Create an expected totals datastructure
        // 2a.  -> Fill the ds with expected values.
        // 3. Construct a PedSumTask AND call initTotals
        // 4. Pass testLine to accumulateTotals()
        // 5. Compare the actual totals to the expected.

        final int columns = 10; // Should be half the number of entries per line.

        // 1.
        final String lineHeader = "col1\tcol2\tcol3\tcol4\tcol5\tcol6\t";

        final String dataLineOne = "A\tC\tT\tC\tT\tG\tA\tC\tT\tG\t0\tG\tC\t0\tG\tA\tC\tT\tG\tA";
        final String dataLineTwo = "G\tC\tT\tG\tA\tC\tT\tA\tA\tC\tT\tG\tA\tC\tT\tG\t0\tG\tC\t0";
        final String dataLineThree = "A\tA\tC\tT\tG\t0\tG\tC\t0\tC\tT\tG\tA\tC\tT\tG\tA\tC\tT\tG";

        final String testLineOne = lineHeader + dataLineOne + "\n";
        final String testLineTwo = lineHeader + dataLineTwo + "\n";
        final String testLineThree = lineHeader + dataLineThree + "\n";

        // 2. Initialize ds:
        List<int[]> expectedTotals = Collections.synchronizedList(new ArrayList<>());
        synchronized (expectedTotals) {
            for (int i = 0; i < columns; ++i) {
                expectedTotals.add(i, new int[NUMBER_OF_BASES]);
            }
        }

        // 2a. Load ds - Sum columns:
        // 
        // L1:   "A C  T C  T G  A C  T G  0 G  C 0  G A  C T  G A"
        // L2:   "G C  T G  A C  T A  A C  T G  A C  T G  0 G  C 0"
        // L3:   "A A  C T  G 0  G C  0 C  T G  A C  T G  A C  T G"
        //
        // A:      3    0    1    2    1    0    2    1    1    1
        // C:      2    2    1    2    2    0    3    0    2    1
        // G:      1    1    2    1    1    3    0    3    1    2
        // T:      0    3    1    1    1    2    0    2    1    1
        // 0:      0    0    1    0    1    1    1    0    1    1
        //
        // Totals: 6    6    6    6    6    6    6    6    6    6

        // Load the expectedTotals data structure with the hand calculated results:
        synchronized (expectedTotals) {
            expectedTotals.get(0)[0] = 3; // A
            expectedTotals.get(0)[1] = 2; // C
            expectedTotals.get(0)[2] = 1; // G
            expectedTotals.get(0)[3] = 0; // T
            expectedTotals.get(0)[4] = 0; // 0/X/N

            expectedTotals.get(1)[0] = 0;
            expectedTotals.get(1)[1] = 2;
            expectedTotals.get(1)[2] = 1;
            expectedTotals.get(1)[3] = 3;
            expectedTotals.get(1)[4] = 0;

            expectedTotals.get(2)[0] = 1;
            expectedTotals.get(2)[1] = 1;
            expectedTotals.get(2)[2] = 2;
            expectedTotals.get(2)[3] = 1;
            expectedTotals.get(2)[4] = 1;

            expectedTotals.get(3)[0] = 2;
            expectedTotals.get(3)[1] = 2;
            expectedTotals.get(3)[2] = 1;
            expectedTotals.get(3)[3] = 1;
            expectedTotals.get(3)[4] = 0;

            expectedTotals.get(4)[0] = 1;
            expectedTotals.get(4)[1] = 2;
            expectedTotals.get(4)[2] = 1;
            expectedTotals.get(4)[3] = 1;
            expectedTotals.get(4)[4] = 1;

            expectedTotals.get(5)[0] = 0;
            expectedTotals.get(5)[1] = 0;
            expectedTotals.get(5)[2] = 3;
            expectedTotals.get(5)[3] = 2;
            expectedTotals.get(5)[4] = 1;

            expectedTotals.get(6)[0] = 2;
            expectedTotals.get(6)[1] = 3;
            expectedTotals.get(6)[2] = 0;
            expectedTotals.get(6)[3] = 0;
            expectedTotals.get(6)[4] = 1;

            expectedTotals.get(7)[0] = 1;
            expectedTotals.get(7)[1] = 0;
            expectedTotals.get(7)[2] = 3;
            expectedTotals.get(7)[3] = 2;
            expectedTotals.get(7)[4] = 0;

            expectedTotals.get(8)[0] = 1;
            expectedTotals.get(8)[1] = 2;
            expectedTotals.get(8)[2] = 1;
            expectedTotals.get(8)[3] = 1;
            expectedTotals.get(8)[4] = 1;

            expectedTotals.get(9)[0] = 1;
            expectedTotals.get(9)[1] = 1;
            expectedTotals.get(9)[2] = 2;
            expectedTotals.get(9)[3] = 1;
            expectedTotals.get(9)[4] = 1;
        }

        // 3.
        PedSumTask pedSumTask = new PedSumTask(TEST_INPUT_PED, 0, 3, columns);
        Method initTotals = PedSumTask.class.getDeclaredMethod("initTotals");
        initTotals.setAccessible(true);
        initTotals.invoke(pedSumTask);

        // 4.
        Method accumulateTotals = PedSumTask.class.getDeclaredMethod("accumulateTotals", String.class);
        accumulateTotals.setAccessible(true);
        accumulateTotals.invoke(pedSumTask, testLineOne);
        accumulateTotals.invoke(pedSumTask, testLineTwo);
        accumulateTotals.invoke(pedSumTask, testLineThree);

        /* Manually print both data structures:
        for (int i = 0; i < expectedTotals.size(); ++i) {
            System.out.print("(" + i + ") ");
            for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                System.out.print(" " + expectedTotals.get(i)[j]);
            }
            System.out.println("");
        }
        for (int i = 0; i < (pedSumTask.getTotals()).size(); ++i) {
            System.out.print("(" + i + ") ");
            for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                System.out.print(" " + (pedSumTask.getTotals()).get(i)[j]);
            }
            System.out.println("");
        }
        */

        // 5.
        synchronized (expectedTotals) {
            Assertions.assertAll(
                () -> assertTrue((pedSumTask.getTotals()).size() > 0),
                () -> assertTrue(expectedTotals.size() > 0),
                () -> assertEquals(expectedTotals.size(), (pedSumTask.getTotals()).size()),
                () -> {
                    for (int i = 0; i < expectedTotals.size(); ++i) {
                        for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                            assertEquals(expectedTotals.get(i)[j], (pedSumTask.getTotals()).get(i)[j]);
                        }
                    }
                }
            );
        }
    }

}
