package com.snptools.converter.pedutilities;

import com.snptools.converter.fileutilities.DiskFullException;
import com.snptools.converter.fileutilities.FileController;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

/**
 * The PedController class directs the conversion of a .ped text file into a .csv output file.
 * The frequencies of bases are first totaled and then the major allele is determined from the data set
 * provided. The ped file is compared against the calculated major to determine the csv output.
 * @author  Jeff Warner
 * @version 1.2, September 2021
 */
public class PedController {

    private final int NUMBER_OF_HEADER_COLUMNS = 6; // Number of columns in ped header.
    private final int NUMBER_OF_BASES = 5; // The number of bases/SNPs to be included in the totals ds: ACTG0.
    private volatile String pedFileName = "./input.ped"; // The input file name with path and extension.
    private volatile String outputFileName = "./output.csv"; // The output file name with path and extension.
    private final String TEMP_FILE_NAME = "TEMP"; // Appendix for intermediate files.
    private volatile int totalInputLines = 0;
    private volatile int inputColumnCount = 0; // = totals.size().
    private final int NUMBER_OF_WORKERS = 4; // The number of threads to create.

    //  ALL access, including reading through .get(), must be in a synchronized block or through the synchronized method.
    private List<int[]> totals = Collections.synchronizedList(new ArrayList<>()); // This is the main data structure holding frequencies of each pair of alleles.

    volatile String[] majorAllelesValues; // The actual major allele for this pair of alleles.

    private volatile PedSumTask[] sumPool; // The pool to hold the pre-processing workers.
    private volatile PedResultsTask[] resultsPool; // The pool to hold the results processing workers.

    /**
     * Constructor for the PedController class - can be used to read and convert .ped/.plk.ped files.
     * @param inputName The input file path with file name and an extension.
     * @param outputName    The output file path with file name and an extension.
     */
    public PedController(String inputName, String outputName) {
        this.pedFileName = inputName;
        this.outputFileName = outputName;
    }

    public void startPedToCsv() {
        totalInputLines = FileController.countTotalLines(pedFileName);
        inputColumnCount = countLineLength();

        printDebugLineInfo();
        // Simple integrity check:
        if (totalInputLines <= 0 || inputColumnCount <= 0) {
            System.out.println("\nThe line headers are missing. Could not detect column titles. Please make sure the ped file is valid.\n");
            return;
        }
        processInputThreaded(NUMBER_OF_WORKERS);

        initTotals();
        mergeThreadTotals(); // Grabs the data from the individual task workers and combines them.
        calculateMajors();

        calculateResultsThreaded(NUMBER_OF_WORKERS); // 440ms - Scan the file comparing the file entry to the entry in majorAlleles.
        try {
            mergeFiles(NUMBER_OF_WORKERS, outputFileName, outputFileName + TEMP_FILE_NAME); // Writes the final output.
            cleanUp(); // Attempt to clean up temporary files.
        } catch (DiskFullException e) {
            System.out.println("Error: An IOException occurred - the disk may be full.");
            System.out.println("\nWarning: Partial results are available but not may not be valid.\n");
            e.printStackTrace();
        }

        //printTotals();
    }

    /**
     * Determines the number of data input columns in the set file and returns the result.
     * @return  The number of columns counted in the file. Returns 0 on an error.
     */
    int countLineLength() {
        try (BufferedReader reader = new BufferedReader(new FileReader(pedFileName))) {
            int lineLength = 0;
            String line = reader.readLine();
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                while (lineScanner.hasNext()) {
                    ++lineLength;
                    lineScanner.next();
                }
                lineScanner.close();
            }
            //inputColumnCount = (lineLength - NUMBER_OF_HEADER_COLUMNS) / 2; // Save the total.
            return ((lineLength - NUMBER_OF_HEADER_COLUMNS) / 2); // Save the total.
        } catch (FileNotFoundException e) {
            System.out.println("The input .ped file could not be found or could not be opened.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Creates multiple workers to sum the frequencies of SNPs in the input .ped data file.
     * <p>
     * A pool of workers is created and retained in order to retrieve their results afterwards for further computation.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    private void processInputThreaded(int workers) {
        if (workers > 0) {
            sumPool = new PedSumTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and then start it immediately.
            // Split work evenly, give threads start and end lines
            for (int i = 0; i < workers; ++i) {
                sumPool[i] = new PedSumTask(
                    pedFileName,
                    (i * totalInputLines / workers),
                    (((1 + i) * totalInputLines) / workers),
                    inputColumnCount
                );
                threadPool[i] = new Thread(sumPool[i]);
                threadPool[i].start();
            }
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("Error joining summing worker [" + i + "].");
                    System.out.println("Results are not accurate.");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Initializes and fills the totals datastructure with default data for further transformation.
     */
    private void initTotals() {
        // Populate the totals of arrays to store counts of ACTG0.
        synchronized (totals) {
            for (int i = 0; i < inputColumnCount; ++i) {
                totals.add(i, new int[NUMBER_OF_BASES]); // = 5.
            }
        }
    }

    /**
     * Collects and merges the results from the PedSumTasks contained in the sumPool.
     * <p>
     * The totals datastructure gets transformed into MajorAlleles and that is
     * passed to ResultsTasks to be read for its calculations.
     */
    private void mergeThreadTotals() {
        synchronized(totals) {
            for (PedSumTask task : sumPool) {
                for (int i = 0; i < task.getTotals().size(); ++i) {
                    for (int j = 0; j < NUMBER_OF_BASES; ++j) { // = 5.
                        totals.get(i)[j] += task.getTotals().get(i)[j];
                    }
                }
            }
        }
    }

    /**
     * Initialize and determines major alleles for each site based on the totals datastructure.
     * <p>
     * Simply, converts the numerical totals into a single base for each site. Also, filters out
     * unknown "X" or "0" valued SNPs at the moment.
     */
    private void calculateMajors() {
        majorAllelesValues = new String[inputColumnCount];
        Dictionary<Integer, String> dictionary = new Hashtable<Integer, String>();
        dictionary.put(0, "A");
        dictionary.put(1, "C");
        dictionary.put(2, "G");
        dictionary.put(3, "T");
        dictionary.put(4, "X");
        synchronized (totals) {
            for (int i = 0; i < inputColumnCount; ++i) {
                int max = totals.get(i)[0]; // Initialize the first value as the current most frequent.
                int index = 0;
                for (int j = 0; j < NUMBER_OF_BASES - 1; ++j) { // = 4. Filter out unknown values by not checking index 4.
                    int value = totals.get(i)[j];
                    if (max < value) {
                        max = value;
                        index = j;
                    }
                }
                majorAllelesValues[i] = dictionary.get(index);
            }
        }
    }

    /**
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * <p>
     * Results are written to a .csv text file at the provided location. The result files are meant to be merged sequentially
     * after all threads have completed.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    private void calculateResultsThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new PedResultsTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and then start it immediately.
            // Split work evenly, give threads start and end lines
            for (int i = 0; i < workers; ++i) {
                // (i * lines / arg), (((1 + i) * lines) / arg)
                resultsPool[i] = new PedResultsTask(
                    pedFileName,
                    outputFileName + TEMP_FILE_NAME + i,
                    (i * totalInputLines / workers),
                    (((1 + i) * totalInputLines) / workers),
                    inputColumnCount,
                    majorAllelesValues
                );
                threadPool[i] = new Thread(resultsPool[i]);
                threadPool[i].start();
            }
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("There was an error joining results worker [" + i + "].");
                    System.out.println("\nResults are not accurate.\n");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * All access to totals must be synchronized. Including reading of data through .get().
     *
     * This is the main DS that holds the frequency summation results.
     *
     * @return  The synchronized totals datastructure.
     */
    public synchronized List<int[]> getTotals() {
        return totals;
    }

    /**
     * Merges the set of files into a single file at the provided path and name.
     *
     * @param count The number of files in the set, from 0 to count - 1, inclusive.
     * @param resultFile    The output file name with path and an extension.
     * @param tempName  The intermediate file containing its appendix, file path, and an extension.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     * 
     * Note: Will overwrite existing data with no warning or prompts.
     */
    private void mergeFiles(int count, String resultFile, String tempName) throws DiskFullException {
        FileController.mergeFiles(count, resultFile, tempName);
    }

    /**
     * Attempts to clean up a set of temporary files.
     *
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    private void cleanUp() {
        FileController.cleanUp(NUMBER_OF_WORKERS, outputFileName, TEMP_FILE_NAME); // Delete stage 1 files.
    }

    // Debug helper
    private void printDebugLineInfo() {
        System.out.println("Converting file: [" + pedFileName + "] to [" + outputFileName + "]");
        System.out.println("Lines: " + totalInputLines);
        System.out.println("Header line length: " + NUMBER_OF_HEADER_COLUMNS);
        System.out.println("Line length: " + inputColumnCount);
    }

    /**
     * Returns the input file path with an extension.
     *
     * @return  The input file name and path with an extension.
     */
    public String getInputFilePath() {
        return pedFileName;
    }

    /**
     * Returns the output file path with an extension.
     *
     * @return  The output file name and path with an extension.
     */
    public String getOutputFilePath() {
        return outputFileName;
    }

}
