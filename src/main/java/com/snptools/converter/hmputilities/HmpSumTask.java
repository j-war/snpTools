package com.snptools.converter.hmputilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * The HmpSumTask class is used count the frequencies of bases for a set of lines from an .hmp file.
 * The results are retrieved through the synchronized totals datastructure once the task has
 * completed and the thread was successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, August 2021
 */
public class HmpSumTask implements Runnable {

    private final String inputFilename; // The input file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int totalColumns;

    private List<int[]> totals = Collections.synchronizedList(new ArrayList<>()); // All access, including reading through .get(), must be in a synchronized block.

    /**
     * Counts and stores the frequencies of bases for the assigned lines.
     * 
     * @param filename  The input file name with path and extension for processing.
     * @param startLine The line for this worker to start at.
     * @param endLine   The line for this worker to finish at.
     * @param totalColumns  The number of columns that this worker should sum.
     */
    public HmpSumTask(String filename, int startLine, int endLine, int totalColumns) {
        this.inputFilename = filename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.totalColumns = totalColumns;
        //initTotals(); // Called in run()/start().
    }

    /**
     * Initializes and fills the totals datastructure with default data for further transformation.
     */
    private void initTotals() {
        // Populate the totals of arrays to store counts of ACTGN??
        synchronized (totals) {
            for (int i = 0; i < endLine - startLine; ++i) {
                totals.add(i, new int[5]);
            }
        }
    }

    public void run() {
        // 1. Open file and initTotals.
        // 2. Skip to appropriate startLine
        // 3. for the number of lines assigned loop:
        //
        // 4. scan in line and pass to function
        // 5. (for the number of columns on line)
        //
        // 6. get its value
        // 7. check its value and increment appropriate entry  
        // 8.     -> go to next column assigned
        // 9.  -> go to 3.

        // 1.
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilename))) {
            initTotals();
            // 2.
            for (int i = 0; i < startLine; ++i) { reader.readLine(); } // Skip ahead to starting line.
            // 3.
            for (int i = 0; i < endLine - startLine; ++i) {
                // 4.
                accumulateTotals(i, reader.readLine());
            }
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file in a HmpSumTask worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the intermediate file.");
            e.printStackTrace();
        }
    }

    /**
     * Counts and accumulates SNP frequencies from the provided line into the
     * synchronized totals data structure.
     * 
     * @param lineNumber  The line's number that is being parsed.
     * @param line  The TSV line read from the file that will be tallied.
     */
    private void accumulateTotals(int lineNumber, String line) {
        if (line == null || line.isBlank() || line.isEmpty()) {
            System.out.println("The provided line contained no data.");
            return;
        }
        Scanner lineScanner = new Scanner(line);
        lineScanner.useDelimiter(",");
        // 5.
        for (int j = 0; j < totalColumns; ++j) {
            //System.out.println("Line: " + line);
            synchronized (totals) {
                //for (int k = 0; k < columns; ++k) {
                if (lineScanner.hasNext()) {
                    // 6.
                    String value = lineScanner.next();
                    //System.out.println("Value: [" + value + "] ");
                    // Switch on the value to count frequency:
                    // 7.
                    switch (value.substring(0, 1)) {
                        case "A":
                            ++(totals.get(lineNumber)[0]);
                            break;
                        case "C":
                            ++(totals.get(lineNumber)[1]);
                            break;
                        case "T":
                            ++(totals.get(lineNumber)[2]);
                            break;
                        case "G":
                            ++(totals.get(lineNumber)[3]);
                            break;
                        default: // Unknowns.
                            ++(totals.get(lineNumber)[4]);
                            //System.out.println("00, or unknown." + columnNumber + ". " + value);
                            break;
                    }
                    switch (value.substring(1, 2)) {
                        case "A":
                            ++(totals.get(lineNumber)[0]);
                            break;
                        case "C":
                            ++(totals.get(lineNumber)[1]);
                            break;
                        case "T":
                            ++(totals.get(lineNumber)[2]);
                            break;
                        case "G":
                            ++(totals.get(lineNumber)[3]);
                            break;
                        default: // Unknowns.
                            ++(totals.get(lineNumber)[4]);
                            //System.out.println("00, or unknown." + columnNumber + ". " + value);
                            break;
                    }
                } else {
                    System.out.println("Malformed hmp file - there is an unexpected number of entries.");
                    lineScanner.close();
                    return;
                }
            }
        }
        lineScanner.close();
    }

    /**
     * All access to totals must be synchronized. Including reading of data through .get().
     * 
     * @return  The synchronized totals datastructure.
     */
    public synchronized List<int[]> getTotals() {
        return totals;
    }

}
