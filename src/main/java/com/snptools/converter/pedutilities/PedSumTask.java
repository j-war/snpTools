package com.snptools.converter.pedutilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * The PedSumTask class is used count the frequencies of bases for a set of columns.
 * The results are retrieved through the synchronized totals datastructure after the
 * task has completed and the thread was successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class PedSumTask implements Runnable {

    private final int COLUMNS_TO_SKIP = 6; // Number of columns in the ped line header.
    private final int NUMBER_OF_BASES = 5; // Number of bases to create a sum for: ACTG0/X.
    private final String inputFilename; // The input file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int columns;
    private List<int[]> totals = Collections.synchronizedList(new ArrayList<>()); // ALL access, including reading through .get(), must be in a synchronized block.

    /**
     * Count and store the frequencies of bases for the assigned columns.
     * 
     * @param filename  The input file path and file name with a file extension for processing.
     * @param startLine The line for this worker to start at.
     * @param endLine    The line for this worker to finish at.
     * @param columns   The number of column pairs that this worker should sum.
     */
    public PedSumTask(String filename, int startLine, int endLine, int columns) {
        this.inputFilename = filename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.columns = columns;
        //initTotals(); // Called in run()/start().
    }

    /**
     * Initializes and fills the totals datastructure with default data for further transformation.
     */
    private void initTotals() {
        // Populate the totals of arrays to store counts of ACTG0.
        synchronized (totals) {
            for (int i = 0; i < columns; ++i) {
                totals.add(i, new int[NUMBER_OF_BASES]);
            }
        }
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilename))) {
            initTotals();
            for (int i = 0; i < startLine; ++i) { reader.readLine(); } // Skip ahead to starting position.
            for (int i = 0; i < endLine - startLine; ++i) {
                accumulateTotals(reader.readLine());
            }
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the input file.");
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Index does not match expectation. Possible malformed ped file.");
            e.printStackTrace();
        }

    }

    /**
     * Counts and accumulates SNP frequencies from the provided line into the
     * synchronized totals data structure.
     * 
     * @param line  The TSV line read from the file that will be tallied.
     */
    private void accumulateTotals(String line) {
        if (line == null || line.isBlank() || line.isEmpty()) {
            System.out.println("The provided line contained no data.");
            return;
        }
        Scanner lineScanner = new Scanner(line);
        for (int j = 0; j < COLUMNS_TO_SKIP; ++j) { // Skip line header.
            if (lineScanner.hasNext()) { lineScanner.next(); } // else, fall through.
        }
        synchronized (totals) {
            for (int k = 0; k < columns; ++k) {
                String value = "";
                if (lineScanner.hasNext()) {
                    value = lineScanner.next();
                    // Switch on the value to count frequency:
                    switch (value) {
                        case "A", "a":
                            ++(totals.get(k)[0]);
                            break;
                        case "C", "c":
                            ++(totals.get(k)[1]);
                            break;
                        case "G", "g":
                            ++(totals.get(k)[2]);
                            break;
                        case "T", "t":
                            ++(totals.get(k)[3]);
                            break;
                        default: // Unknowns.
                            ++(totals.get(k)[4]);
                            //System.out.println("00, or unknown." + columnNumber + ". " + value);
                            break;
                    }
                } else {
                    System.out.println("Malformed ped file: unexpected number of alleles.");
                    lineScanner.close();
                    return;
                }
                if (lineScanner.hasNext()) {
                    value = lineScanner.next();
                    switch (value) {
                        case "A", "a":
                            ++(totals.get(k))[0];
                            break;
                        case "C", "c":
                            ++(totals.get(k))[1];
                            break;
                        case "G", "g":
                            ++(totals.get(k))[2];
                            break;
                        case "T", "t":
                            ++(totals.get(k))[3];
                            break;
                        default: // Unknowns.
                            ++(totals.get(k)[4]);
                            //System.out.println("00, or unknown." + columnNumber + ". " + value);
                            break;
                    }
                } else {
                    System.out.println("Malformed ped file, odd number of alleles.");
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
