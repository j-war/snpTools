package com.snptools.converter.fileutilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for FileController.
 * 
 * @author  Jeff Warner
 * @version 1.0, August 2021
 */
public class FileControllerTest {

    final String TEST_INPUT_PED = "./src/test/resources/test.ped";
    final String TEST_OUTPUT_PED = "./src/test/resources/testPed.csv";
    final String TEST_DNE_FILE = "./src/test/resources/dne.csv";
    final String TEST_EMPTY_FILE = "./src/test/resources/empty.csv";

    final String DOES_NOT_EXIST = "DOES_NOT_EXIST";
    final File FILE_DOES_NOT_EXIST = new File("not a path");
    final int FILE_COUNT = 4; // The number of sequential files that should be written.

    final int START_LINE = 0;
    final int END_LINE = 3;
    final int COLUMN_PAIRS = 474; // is equal to half the number of data columns in the test file.
    // test.ped has 474 * 2 data entries = 948.

    /**
     * Tests that the test input file exists and can be accessed.
     */
    @Test
    @DisplayName("testCanReadFile")
    void testCanReadFile() {
        File inFile = new File(TEST_INPUT_PED);
        Assertions.assertAll(
            () -> assertTrue(inFile.exists()),
            () -> assertTrue(FileController.canReadFile(TEST_INPUT_PED)),

            () -> assertFalse(FileController.canReadFile(DOES_NOT_EXIST + TEST_INPUT_PED))
        );
    }

    /**
     * Tests that the expected number of lines is returned from the FileController.
     * Including files that do not exist or are empty.
     */
    @Test
    @DisplayName("testCountTotalLines")
    void testCountTotalLines() {
        assertEquals((END_LINE - START_LINE), FileController.countTotalLines(TEST_INPUT_PED));
        assertEquals(-1, FileController.countTotalLines(TEST_DNE_FILE));
        assertEquals(0, FileController.countTotalLines(TEST_EMPTY_FILE));
    }

    /**
     * Tests that the test input and output folders exist and can be accessed.
     */
    @Test
    @DisplayName("testDirectoryExists")
    void testDirectoryExists() {
        File inFile = new File(TEST_INPUT_PED);
        File outFile = new File(TEST_OUTPUT_PED);
        File inFolder = new File(inFile.getParent());
        File outFolder = new File(outFile.getParent());

        Assertions.assertAll(
            () -> assertTrue(inFolder.exists()),
            () -> assertTrue(inFolder.isDirectory()),
            () -> assertTrue(outFolder.exists()),
            () -> assertTrue(outFolder.isDirectory()),
            () -> assertEquals(null, FILE_DOES_NOT_EXIST.getParent()),
    
            () -> assertTrue(FileController.directoryExists(TEST_INPUT_PED)),
            () -> assertTrue(FileController.directoryExists(TEST_OUTPUT_PED)),

            () -> assertFalse(FileController.directoryExists(DOES_NOT_EXIST + TEST_INPUT_PED)),
            () -> assertFalse(FileController.directoryExists(DOES_NOT_EXIST + TEST_OUTPUT_PED))
        );
    }

    /**
     * Tests that the FileController's mergeFiles() method properly merges
     * the provided file list.
     */
    @Test
    @DisplayName("testMergeFiles")
    void testMergeFiles() {
        writeFiles(FILE_COUNT);
        Assertions.assertAll(
            () -> {
                for (int i = 0; i < FILE_COUNT; ++i) {
                    assertTrue((new File(TEST_OUTPUT_PED + i)).exists());
                }
            }
        );
        try {
            FileController.mergeFiles(FILE_COUNT, TEST_OUTPUT_PED, TEST_OUTPUT_PED);
        } catch (DiskFullException e) {
            System.out.println("There was an error while running tests - disk may be full.");
        }
        // Check that the file exists after merge,
        // Check that the file contains lines by counting the lines,
        // Check that the file doesn't contain 1 or 0 lines.
        Assertions.assertAll(
            () -> assertTrue((new File(TEST_OUTPUT_PED)).exists()),
            () -> assertEquals(FILE_COUNT * (END_LINE - START_LINE), FileController.countTotalLines(TEST_OUTPUT_PED)),

            () -> assertTrue( FileController.countTotalLines(TEST_OUTPUT_PED) != 0 ),
            () -> assertTrue( FileController.countTotalLines(TEST_OUTPUT_PED) != 1 )
        );
    }

    /**
     * Tests that deleteSingleFile() method does in fact delete the
     * single file.
     */
    @Test
    @DisplayName("testDeleteSingleFile")
    void testDeleteSingleFile() {
        // write a file
        // test it is there
        // delete that file
        // test it is not there.
        writeFiles(1);

        assertTrue((new File(TEST_OUTPUT_PED + 1)).exists());
        FileController.deleteSingleFile(TEST_OUTPUT_PED + 1);
        assertFalse((new File(TEST_OUTPUT_PED + 1)).exists());
    }

    /**
     * Tests that the sequence of files is properly deleted by first writing
     * a set of files, checking that they exist, deleting that set, followed
     * by checking that they no longer exist.
     */
    @Test
    @DisplayName("testCleanUp")
    void testCleanUp() {
        writeFiles(FILE_COUNT);
        Assertions.assertAll(
            () -> {
                for (int i = 0; i < FILE_COUNT; ++i) {
                    assertTrue((new File(TEST_OUTPUT_PED + i)).exists());
                }
            }
        );
        FileController.cleanUp(FILE_COUNT, TEST_OUTPUT_PED, "");
        Assertions.assertAll(
            () -> {
                for (int i = 0; i < FILE_COUNT; ++i) {
                    assertFalse((new File(TEST_OUTPUT_PED + i)).exists());
                }
            }
        );
    }

    /**
     * Note: Uses a cast from int to char for some checks.
     * 
     * Tests that the expected byte/int range is correctly interpreted and
     * filtered to the acceptable values.
     */
    @Test
    @DisplayName("testIntToChar")
    void testIntToChar() {
        // Create array of ints inside and outside of acceptable range
        // Create array of chars with expected values
        // Compare expected and actual arrays
        final int[] intsToTest = new int[128];
        final char[] charsToCheck = new char[128];
        final char[] expectedChars = new char[128];
        for (int i = 0; i < 128; ++i) {
            intsToTest[i] = i;
            expectedChars[i] = (char) i;
            charsToCheck[i] = FileController.intToChar(intsToTest[i]);
        }

        // Check most values:
        Assertions.assertAll(
            () -> {
                for (int i = 0; i < 43; ++i) {
                    assertEquals('X', charsToCheck[i]);
                }
            },
            () -> {
                for (int i = 43; i < 59; ++i) {
                    assertEquals(expectedChars[i], charsToCheck[i]);
                    assertEquals((char) i, charsToCheck[i]);
                }
            },
            () -> {
                assertEquals('A', charsToCheck[65]);
                assertEquals('A', charsToCheck[97]);

                assertEquals('C', charsToCheck[67]);
                assertEquals('C', charsToCheck[99]);

                assertEquals('G', charsToCheck[71]);
                assertEquals('G', charsToCheck[103]);

                assertEquals('N', charsToCheck[78]);
                assertEquals('N', charsToCheck[110]);

                assertEquals('T', charsToCheck[84]);
                assertEquals('T', charsToCheck[116]);

                assertEquals('|', charsToCheck[124]);

                assertEquals('X', charsToCheck[125]);
                assertEquals('X', charsToCheck[126]);
                assertEquals('X', charsToCheck[127]);
            }
        );

    }

    /**
     * Convenience method for writing a sequence of simple text files.
     * 
     * @param count The number of files that should be written.
     * 
     * Note: Will overwrite files with no warnings or prompts.
     */
    private void writeFiles(int count) {
        for (int i = 0; i < count; ++i) {
            try (
                FileOutputStream outputStream = new FileOutputStream(TEST_OUTPUT_PED + i);
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
            ) {
                outputStreamWriter.write("Line 1 from file: " + i + ".\n");
                outputStreamWriter.write("Line 2 from file: " + i + ".\n");
                outputStreamWriter.write("Line 3 from file: " + i + ".\n");
            } catch (FileNotFoundException e) {
                System.out.println("The provided file could not be found or it could not be opened.");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("There was a problem accessing the input file or writing to the output.");
                e.printStackTrace();
            }
        }
    }

}
