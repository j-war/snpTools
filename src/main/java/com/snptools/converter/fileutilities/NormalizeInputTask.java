package com.snptools.converter.fileutilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.FileLock;
import java.util.Scanner;

/**
 * The NormalizeInputTask class is used to strip file and line headers from a text input file
 * for further processing in the program. The result is written to a seperate output file that
 * is meant to be merged with the results of other NormalizeInputTask outputs.
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class NormalizeInputTask implements Runnable {

    private final String inputFilenameWithExt;
    private final String outputFilenameWithExt;
    private final int startLine;
    private final int endLine;
    private final int startColumn;
    private final int numberOfColumns;
    private final int columnWidth;
    private String[] partialResults;

    /**
     * Normalizes the input data by stripping file and line headers based on the provided constraints.
     * @param inputFilenameWithExt  The input file path and file name with a file extension for processing.
     * @param outputFilenameWithExt  The output file path and file name with a file extension to be written.
     * @param startLine The line for this worker to start at.
     * @param endLine   The line for this worker to end at.
     * @param startColumn   The starting input column for this worker to start at.
     * @param numberOfColumns   The number of columns that should be kept.
     * @param columnWidth   The number of characters per column.
     */
    public NormalizeInputTask(String inputFilenameWithExt, String outputFilenameWithExt, int startLine, int endLine, int startColumn, int numberOfColumns, int columnWidth) {
        this.inputFilenameWithExt = inputFilenameWithExt;
        this.outputFilenameWithExt = outputFilenameWithExt;
        this.startLine = startLine;
        this.endLine = endLine;
        this.startColumn = startColumn;
        this.numberOfColumns = numberOfColumns;
        this.columnWidth = columnWidth;
        this.partialResults = new String[numberOfColumns];
    }

    @Override
    public void run() {
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilenameWithExt));
            FileOutputStream outputStream = new FileOutputStream(outputFilenameWithExt);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            FileLock lock = outputStream.getChannel().tryLock();

            if (lock != null) {
                for (int i = 0; i < startLine; ++i) { reader.readLine(); } // Skip ahead to starting point.
                for (int i = 0; i < endLine - startLine; ++i) {
                    accumulateResults(reader.readLine());
                    // Write the accumulated results:
                    for (int x = 0; x < numberOfColumns; ++x) {
                        if (x < numberOfColumns - 1) {
                            outputStreamWriter.write(partialResults[x] + ",");
                        } else {
                            outputStreamWriter.write("" + partialResults[x]);
                        }
                    }
                    outputStreamWriter.write("\n");
                }
                lock.release();
            }
        } catch (FileNotFoundException e) {
            System.out.println("A normalization worker could not find the provided file.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("A normalization worker had a problem reading the input file for processing.");
            e.printStackTrace();
        } catch (SecurityException e) {
            System.out.println("Could not access a file from a normalization worker. Please check permissions and the security manager.");
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("A normalization worker had a problem reading the input. Column size does not match expectations.");
            e.printStackTrace();
        }
    }

    /**
     * Parses the provided line and strips the trailing characters that occur
     * after the first 0 to columnWidth characters. The results are stored in
     * the partialResults line buffer.
     * 
     * @param line The data line that will be parsed and stripped.
     */
    private void accumulateResults(String line) {
        if (line == null || line.isBlank() || line.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            for (int k = 0; k < numberOfColumns; ++k) {
                partialResults[k] = "";
            }
            return;
        }
        Scanner lineScanner = new Scanner(line);
        for (int j = 0; j < startColumn; ++j) {
            if (lineScanner.hasNext()) { lineScanner.next(); } // else, fall through.
        } // Skip the line header.
        // Parse line into partialResults:
        for (int k = 0; k < numberOfColumns; ++k) {
            String value = "";
            if (lineScanner.hasNext()) {
                value = lineScanner.next();
            } else {
                System.out.println("Malformed input file, number of inputs does not match expected amount.");
                lineScanner.close();
                return;
            }
            if (value.length() >= columnWidth) {
                partialResults[k] = value.substring(0, columnWidth); // Only need the GT (genotype) information, drop the rest.
            } else { // else, data entry appears invalid
                partialResults[k] = "";
            }
        }
        lineScanner.close();
    }

}
