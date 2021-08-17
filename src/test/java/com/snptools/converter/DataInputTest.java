package com.snptools.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit test for DataInput.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class DataInputTest {

    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";
    final String DOES_NOT_EXIST = "DOES_NOT_EXIST";
    final File FILE_DOES_NOT_EXIST = new File("not a path");

    /**
     * JUnit5 setup check.
     */
    @Test
    @DisplayName("shouldAnswerWithTrue")
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    /**
     * Simple setup check.
     */
    @Test
    @DisplayName("shouldCheckAllInputArguments")
    void shouldCheckAllInputArguments() {
        List<String> numbers = List.of("0", "./InputFolder", "./OutputFolder");
        Assertions.assertAll(
            () -> assertEquals("0", numbers.get(0)),
            () -> assertEquals("./InputFolder", numbers.get(1)),
            () -> assertEquals("./OutputFolder", numbers.get(2)),
            () -> assertEquals(3, numbers.size())
        );
    }

    /**
     * Tests whether the input and output test files and folders are accessible
     * to the testing program and whether the main entry point correctly interprets
     * valid and invalid file paths.
     */
    @Test
    @DisplayName("testCanAccessDataFiles")
    void testCanAccessDataFiles() {
        System.out.println("\nNote: Ignore \"Error: Cannot read ... closing.\" print statements while testing.\n");
        // TRUE: Good, good
        // FALSE: Good, bad
        // FALSE: Bad, good
        // FALSE: Bad, bad
        Assertions.assertAll(
            () -> assertTrue(DataInput.canAccessDataFiles(TEST_INPUT_PED, TEST_OUTPUT_PED)),
            () -> assertFalse(DataInput.canAccessDataFiles(TEST_INPUT_PED, DOES_NOT_EXIST + TEST_OUTPUT_PED)),
            () -> assertFalse(DataInput.canAccessDataFiles(DOES_NOT_EXIST + TEST_INPUT_PED, TEST_OUTPUT_PED)),
            () -> assertFalse(DataInput.canAccessDataFiles(DOES_NOT_EXIST + TEST_INPUT_PED, DOES_NOT_EXIST + TEST_OUTPUT_PED))
        );
    }

}
