package com.snptools.converter.hmputilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import com.snptools.converter.fileutilities.DiskFullException;
import com.snptools.converter.fileutilities.FileController;
import com.snptools.converter.fileutilities.NormalizeInputTask;

/**
 * The HmpController class directs the conversion of an .hmp text file into a .csv output file
 * or an .hmp text file into a .vcf output file.
 * @author  Jeff Warner
 * @version 1.3, September 2021
 */
public class HmpController {

    private volatile String hmpFileName = "./input.hmp"; // The input file name with path and extension.
    private volatile String outputFileName = "./output.csv"; // The output file name with path and extension.
    private final String TEMP_FILE_NAME = "TEMP"; // File name appendix for stage 1 - normalized.
    private final String TEMP_FILE_NAME_2ND = "TEMP_2ND"; // File name appendix for stage 2.
    private final int NUMBER_OF_WORKERS = 4; // The number of worker threads to create.
    private final int NUMBER_OF_BASES = 16 + 1; // Number of bases, or combination of bases, to create a sum for, including +1 for unknown values.
    private int hmpPloidiness = 0; // The number of characters in the HMP file's data entries.

    private int totalInputLines = 0; // The total number of lines in the file including the header.
    private int NUMBER_OF_HEADER_LINES = 1; // The size of the file header.
    private int totalInputColumns = 0; // The total number of columns. Number of data columns = totalInputColumns - numberOfheaderColumns
    private final int NUMBER_OF_HEADER_COLUMNS = 11; // The size of the line header.

    // All access, including reading through .get(), must be in a synchronized block.
    private List<int[]> totals = Collections.synchronizedList(new ArrayList<>()); // This is the main data structure holding frequencies of each pair of alleles.
    private List<String[]> lineHeaders = Collections.synchronizedList(new ArrayList<>()); // Line headers from the input file.

    private volatile String[] majorAllelesValues; // The actual major allele for this pair of alleles.
    private volatile String[] allAllelesValues; // The alternate/reference alleles for this site.
    private volatile String[] sampleIds; // The sample ids from the file header.
    private volatile String[] strandDirections; // The strand directions.

    private volatile String[] outputLineHeaders; // The constructed file and line headers for the output file.

    private volatile NormalizeInputTask[] normalizePool; // The pool to hold the data normalizing workers.
    private volatile HmpSumTask[] sumPool; // The pool to hold the summation workers.
    private volatile Runnable[] resultsPool; // The common pool to hold the result conversion workers.

    /**
     * Constructor for the HmpController class - can be used to read and convert .hmp files.
     * @param inputName The input file path with file name and an extension.
     * @param outputName    The output file path with file name and an extension.
     */
    public HmpController(String inputName, String outputName) {
        this.hmpFileName = inputName;
        this.outputFileName = outputName;
    }

    public void startHmpToCsv() {
        if (!prepareInputData()) {
            return; // Failed to prepare data or get file constraints.
        }

        makeMajorAllelesDataStructures();
        convertHmpToCsv();
    }

    private void convertHmpToCsv() {
        if (totalInputLines >= 2500 || totalInputColumns >= 250) { // Arbitrary values.
            System.out.println("\nLarge input file detected.\n");
        }
        convertHmpToCsvLargeThreaded(NUMBER_OF_WORKERS);
        try {
            FileController.mergeFilesLines(
                totalInputColumns - NUMBER_OF_HEADER_COLUMNS,
                NUMBER_OF_WORKERS,
                outputFileName,
                outputFileName + TEMP_FILE_NAME_2ND
            );
            cleanUpAll(); // Attempt to delete temporary files and folders.
        } catch (DiskFullException e) {
            System.out.println("Error: The disk appears to be full. However, partial results are available.");
            System.out.println("Use with caution.");
            System.out.println("Partial results available at:");
            for (int x = 0; x < NUMBER_OF_WORKERS; ++x) {
                System.out.println("File " + (x + 1) + ": [" + outputFileName + TEMP_FILE_NAME_2ND + x + "]");
            }
        }
    }

    public void startHmpToVcf() {
        if (!prepareInputData()) {
            return; // Failed to prepare data or get file constraints.
        }

        String intermediateFile = outputFileName + TEMP_FILE_NAME;
        try {
            mergeFiles(NUMBER_OF_WORKERS, intermediateFile, outputFileName + TEMP_FILE_NAME);
        } catch (DiskFullException e) {
            System.out.println("Error: An IOException occurred - the disk may be full.");
            System.out.println("\nWarning: Results are invalid.\n");
            e.printStackTrace();
        }

        makeMajorAllelesDataStructures();

        // Create headers, exit if unable to do so.
        if (!makeVcfHeaders()) {
            System.out.println("Failed to collect data.");
            return;
        }

        convertHmpToVcf();
    }

    private void convertHmpToVcf() {
        convertHmpToVcfThreaded(NUMBER_OF_WORKERS); // Write output.
        try {
            mergeFiles(NUMBER_OF_WORKERS, outputFileName, outputFileName + TEMP_FILE_NAME_2ND);
            cleanUpAll(); // Attempt to delete temporary files.
        } catch (DiskFullException e) {
            System.out.println("Error: An IOException occurred - the disk may be full.");
            System.out.println("\nWarning: Partial results are available but may not be valid.\n");
            e.printStackTrace();
        }
    }

    /**
     * Collects file constraints and normalizes the data for further processing.
     * @return  Whether the file constraints were consistent
     */
    private boolean prepareInputData() {
        totalInputLines = FileController.countTotalLines(hmpFileName);
        totalInputColumns = countLineLength();

        printDebugLineInfo();
        // Simple integrity check:
        if (totalInputLines <= 0 || totalInputColumns <= 0) {
            System.out.println("\nThe file header is missing or the file is empty. Please make sure the hmp file is valid.\n");
            return false;
        }

        hmpPloidiness = determinePloidiness();
        // Simple integrity check:
        if (hmpPloidiness <= 0) {
            System.out.println("\nThe ploidiness of the file could not be determined. Please make sure the hmp file is valid.\n");
            return false;
        }

        normalizeInputThreaded(NUMBER_OF_WORKERS);
        return true;
    }

    /**
     * Creates and populates the majorAlleles data structures.
     */
    private void makeMajorAllelesDataStructures() {
        processInputThreaded(NUMBER_OF_WORKERS); // Sum frequencies.

        // Determine and set major alleles:
        initTotals();
        mergeThreadTotals();
        calculateMajors();
        //printIntermediateData();
    }

    /**
     * Organizing method that creates the output vcf headers.
     * @return  True if the headers were created, false is there are
     *          any errors.
     */
    private boolean makeVcfHeaders() {
        // Collect the input file line headers:
        initLineHeaders();
        collectLineHeaders();

        collectSampleIds();
        // Simple integrity check: Check if ids were collected, otherwise return
        if (sampleIds == null || (sampleIds.length != (totalInputColumns - NUMBER_OF_HEADER_COLUMNS))) {
            System.out.println("\nSample ids could not be detected. Please make sure the hmp file is well formed and that the number of records matches the number of ids.\n");
            return false;
        }

        collectStrandDirections();
        //printStrandDirections();
        // Simple integrity check: Check if strands were collected, otherwise return
        if (strandDirections == null || (strandDirections.length != (totalInputLines - NUMBER_OF_HEADER_LINES))) {
            System.out.println("\nStrand directions could not be collected. Please make sure the HMP file is well formed.\n");
            return false;
        }

        // Collect all of the alleles into a single ds in order to construct a proper line header as well as to
        // provide translation of bases->integers for the hmp->vcf conversion process.
        collectAllAlleles();
        //printAllAllelesValues();

        createOutputLineHeaders();
        //printOutputLineHeaders();
        if (allAllelesValues == null || outputLineHeaders == null || (outputLineHeaders.length != (totalInputLines - NUMBER_OF_HEADER_LINES))) {
            System.out.println("\nVCF output headers could not be created. Please make sure the HMP file is well formed.\n");
            return false;
        }

        return true;
    }

    /**
     * Determines the number of data input columns in the set file and returns the result.
     * @return  The number of columns counted in the file. Returns 0 on an error.
     */
    private int countLineLength() {
        try (BufferedReader reader = new BufferedReader(new FileReader(hmpFileName))) {
            int lineLength = 0;
            for (int i = 0; i < NUMBER_OF_HEADER_LINES; ++i) { reader.readLine(); } // Skip ahead.
            String line = reader.readLine();
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                while (lineScanner.hasNext()) {
                    lineScanner.next();
                    ++lineLength;
                }
                lineScanner.close();
            }
            //totalInputColumns = lineLength; // Save the total.
            return lineLength;
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Retrieves and saves the line headers of the set input file.
     */
    private void collectLineHeaders() {
        try (BufferedReader reader = new BufferedReader(new FileReader(hmpFileName))) {
            for (int i = 0; i < NUMBER_OF_HEADER_LINES; ++i) { reader.readLine(); } // Skip ahead.
            for (int i = 0; i < totalInputLines - NUMBER_OF_HEADER_LINES; ++i) {
                String line = reader.readLine();
                //System.out.println("Line: [" + line + "]");
                if (line != null) {
                    Scanner lineScanner = new Scanner(line);
                    for (int j = 0; j < NUMBER_OF_HEADER_COLUMNS; ++j) {
                        synchronized (lineHeaders) {
                            if (lineScanner.hasNext()) {
                                String token = lineScanner.next();
                                lineHeaders.get(i)[j] = token;
                            } else {
                                lineScanner.close();
                                return; // Exit early.
                            }
                        }
                    }
                    lineScanner.close();
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an IO error checking the file.");
            e.printStackTrace();
        }
    }

    /**
    * Initializes and fills the lineHeaders datastructure with default data.
    */
    private void initLineHeaders() {
        // Populate the lineHeaders to store sample/entry data.
        synchronized (lineHeaders) {
            for (int i = 0; i < totalInputLines - NUMBER_OF_HEADER_LINES; ++i) {
                lineHeaders.add(i, new String[NUMBER_OF_HEADER_COLUMNS]);
            }
        }
    }

    /**
     * All access to lineHeaders must be synchronized. Including reading of data through .get().
     * 
     * @return  The synchronized lineHeaders datastructure.
     */
    public synchronized List<String[]> getLineHeaders() {
        return lineHeaders;
    }

    /**
     * Creates and saves a string array of sample ids from the input hmp file.
     */
    private void collectSampleIds() {
        try (BufferedReader reader = new BufferedReader(new FileReader(hmpFileName))) {
            sampleIds = new String[totalInputColumns - NUMBER_OF_HEADER_COLUMNS];
            // for (int i = 0; i < numberOfHeaderLines - 1; ++i) { reader.readLine(); } // Skip ahead.
            String line = reader.readLine(); // Should be the line containing sample ids - starts with "#CHROM".
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                for (int i = 0; i < NUMBER_OF_HEADER_COLUMNS; ++i) { // Skip ahead to the sample ids.
                    if (lineScanner.hasNext()) {
                        lineScanner.next();
                    } else {
                        System.out.println("Malformed header detected.");
                        lineScanner.close();
                        return;
                    }
                }

                for (int i = 0; i < totalInputColumns - NUMBER_OF_HEADER_COLUMNS; ++i) {
                    if (lineScanner.hasNext()) {
                        sampleIds[i] = "" + lineScanner.next();
                        //System.out.println("Sample ids [" + i + "]: " + sampleIds[i]);
                    } else {
                        System.out.println("Malformed sample ids detected.");
                        lineScanner.close();
                        return;
                    }

                }
                lineScanner.close();
            }
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an IO error checking the file.");
            e.printStackTrace();
        }
    }

    /**
     * Creates and saves a string array of strand directions from the lineHeaders data structure.
     * SNPs' orientation relative to the reference genome in the:
     *      forward direction : '+'
     *      reverse direction : '-'
     */
    private void collectStrandDirections() {
        strandDirections = new String[totalInputLines - NUMBER_OF_HEADER_LINES];
        for (int i = 0; i < strandDirections.length; ++i) {
            strandDirections[i] = "" + getLineHeaders().get(i)[4];
        }
    }

    /**
     * Determines the level of ploidiness of the provided file by navigating to the first
     * data entry and returning entry.length().
     * Returns 0 on any error.
     * 
     * @return  The ploidiness of the file, simply the length of the data entry string.
     */
    private int determinePloidiness() {
        try (BufferedReader reader = new BufferedReader(new FileReader(hmpFileName))) {
            int ploidiness = 0;
            for (int i = 0; i < NUMBER_OF_HEADER_LINES; ++i) { reader.readLine(); } // Skip ahead.
            String line = reader.readLine();
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                for (int j = 0; j < NUMBER_OF_HEADER_COLUMNS; ++j) { // Skip line header
                    if (lineScanner.hasNext()) {
                        lineScanner.next();
                    } else {
                        lineScanner.close();
                        return 0;
                    }
                }
                // Assuming there is at least one data column following the line header:
                if (lineScanner.hasNext()) {
                    String dataEntry =  lineScanner.next();
                    ploidiness = dataEntry.length();
                    lineScanner.close();
                    return ploidiness;
                } else {
                    lineScanner.close();
                    System.out.println("A data entry could not be found.");
                    return 0;
                }
            } // else, the line was null.
            return 0;
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Normalizes the data input file for uniform processing and file navigation and improved performance.
     * <p>
     * File headers and line headers are stripped according to previously determined constraints. 
     * Work is distributed evenly amongst worker threads.
     * <p>
     * 
     * @param workers   The number of threads and the number of resulting files.
     * 
     * Note: Further explanations can be found in the NormalizeInputTask.java file.
     */
    private void normalizeInputThreaded(int workers) {
        if (workers > 0) {
            normalizePool = new NormalizeInputTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and start it immediately.
            // Split work evenly:
            for (int i = 0; i < workers; ++i) {
                normalizePool[i] = new NormalizeInputTask(
                    hmpFileName,
                    outputFileName + TEMP_FILE_NAME + i,
                    (i * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers) + NUMBER_OF_HEADER_LINES,
                    ((1 + i) * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers) + NUMBER_OF_HEADER_LINES,
                    NUMBER_OF_HEADER_COLUMNS,
                    totalInputColumns - NUMBER_OF_HEADER_COLUMNS,
                    hmpPloidiness // Column width: the number of chars.
                );
                threadPool[i] = new Thread(normalizePool[i]);
                threadPool[i].start();
            }
            // Join and wait for the workers to complete:
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("Error joining normalizing worker [" + i + "].");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Process the intermediate data that has been normalized.
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    private void processInputThreaded(int workers) {
        if (workers > 0) {
            sumPool = new HmpSumTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and then start it immediately.
            // Split work evenly, give threads start and end lines
            for (int i = 0; i < workers; ++i) {
                sumPool[i] = new HmpSumTask(
                    outputFileName + TEMP_FILE_NAME + i,
                    (i * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers),
                    (((1 + i) * (totalInputLines - NUMBER_OF_HEADER_LINES)) / workers),
                    (totalInputColumns - NUMBER_OF_HEADER_COLUMNS),
                    true // Scan the whole file.
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
     * Process the intermediate data that has been normalized.
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * <p>
     * Results are written to a .csv text file at the set file location. The result files are meant to be merged sequentially
     * after all threads have completed.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    private void convertHmpToVcfThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new HmpToVcfTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and start it immediately.
            // Split work evenly:
            for (int i = 0; i < workers; ++i) {
                resultsPool[i] = new HmpToVcfTask(
                    outputFileName + TEMP_FILE_NAME,
                    outputFileName + TEMP_FILE_NAME_2ND + i,
                    (i * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers),
                    ((1 + i) * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers),
                    totalInputColumns - NUMBER_OF_HEADER_COLUMNS,
                    allAllelesValues,
                    strandDirections,
                    outputLineHeaders,
                    hmpPloidiness
                );
                threadPool[i] = new Thread(resultsPool[i]);
                threadPool[i].start();
            }

            // Join and wait for the workers to complete:
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("Error joining processing worker [" + i + "].");
                    System.out.println("Results are not accurate.");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Collects and merges the results from the HmpSumTask contained in the sumPool.
     * <p>
     * The totals datastructure gets transformed into MajorAlleles and that is
     * passed to HmpToCsvTask/HmpToVcfTask to be read for its calculations.
     */
    private void mergeThreadTotals() {
        synchronized (totals) {
            for (int k = 0; k < sumPool.length; ++ k) { // 4.
                for (int i = 0; i < sumPool[k].getTotals().size(); ++i) {
                    int sum = 0;
                    for (int j = 0; j < sumPool[k].getTotals().get(i).length; ++j) { // 17.
                        sum += sumPool[k].getTotals().get(i)[j];
                        totals.get(k * (totalInputLines - NUMBER_OF_HEADER_LINES) / sumPool.length + i)[j] += sumPool[k].getTotals().get(i)[j];
                    }
                    if (sum != hmpPloidiness * (totalInputColumns - NUMBER_OF_HEADER_COLUMNS)) {
                        System.out.println("Invalid sum. Results are not accurate: " + sum);
                    }
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
            for (int i = 0; i < totalInputLines - NUMBER_OF_HEADER_LINES; ++i) {
                totals.add(i, new int[NUMBER_OF_BASES]);
            }
        }
    }

    /**
     * Initialize and determines major alleles for each site based on the totals datastructure.
     * <p>
     * Simply, converts the numerical totals into a single base for each site.
     */
    private void calculateMajors() {
        majorAllelesValues = new String[totalInputLines - NUMBER_OF_HEADER_LINES];
        Dictionary<Integer, String> dictionary = new Hashtable<Integer, String>();
        dictionary.put(0, "A");
        dictionary.put(1, "C");
        dictionary.put(2, "G");
        dictionary.put(3, "T");

        dictionary.put(4, "R");
        dictionary.put(5, "Y");
        dictionary.put(6, "S");
        dictionary.put(7, "W");
        dictionary.put(8, "K");
        dictionary.put(9, "M");

        dictionary.put(10, "B");
        dictionary.put(11, "D");
        dictionary.put(12, "H");
        dictionary.put(13, "V");

        dictionary.put(14, "N");
        dictionary.put(15, "."); // Also includes "-" but we will output as "." only.
        dictionary.put(16, "X");

        synchronized (totals) {
            for (int i = 0; i < totalInputLines - NUMBER_OF_HEADER_LINES; ++i) {
                int max = totals.get(i)[0]; // Initialize the first value as the current most frequent.
                int index = 0;
                for (int j = 0; j < NUMBER_OF_BASES; ++j) {
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
     * Initialize and determines the alternative/reference alleles, as well as the major, for each site based on the
     * totals datastructure. Simply, converts the numerical totals into a single base for each site
     * and places them in frequency descending order.
     * <p>
     * Filters out the unknown/X values as well as SNPS/bases that have a count of 0.
     */
    private void collectAllAlleles() {
        allAllelesValues = new String[totalInputLines - NUMBER_OF_HEADER_LINES];
        Dictionary<Integer, String> dictionary = new Hashtable<Integer, String>(); // Used for easy mapping below.
        dictionary.put(0, "A");
        dictionary.put(1, "C");
        dictionary.put(2, "G");
        dictionary.put(3, "T");

        dictionary.put(4, "R");
        dictionary.put(5, "Y");
        dictionary.put(6, "S");
        dictionary.put(7, "W");
        dictionary.put(8, "K");
        dictionary.put(9, "M");

        dictionary.put(10, "B");
        dictionary.put(11, "D");
        dictionary.put(12, "H");
        dictionary.put(13, "V");

        dictionary.put(14, "N");
        dictionary.put(15, "."); // Also includes "-" but we will output as "." only.
        dictionary.put(16, "X");
        /* 
            Get array entry from totals listing frequencies (index indicates the value ie. "ACTGX")
            Add those entries with ACTGX values to the treemap - they become naturally sorted
                if they key (the frequency of appearing) is duplicate, append to the current result.
            Add the entries to alternateAllelesValues starting at the end of the map if they key > 0.
                only add values (bases/SNPs) that actually appear.
        */
        synchronized (totals) {
            for (int i = 0; i < totalInputLines - NUMBER_OF_HEADER_LINES; ++i) {
                Map<Integer, String> treeMap = new TreeMap<Integer, String>();
                for (int j = 0; j < NUMBER_OF_BASES; ++j) {
                    int freq = totals.get(i)[j];
                    if (treeMap.containsKey(freq)) {
                        String temp = treeMap.get(freq);
                        treeMap.put(freq, "" + temp + "," + dictionary.get(j)); // Append the SNP that has a duplicate frequency.
                    } else {
                        treeMap.put(freq, "" + dictionary.get(j)); // Otherwise, just add the freq+snp to the map.
                    }
                }

                String result = "";
                for (Integer key : treeMap.keySet()) {
                    if (key > 0) { // Only keys greater than 0 to filter out SNPs that do not occur at this site.
                        if (result.length() > 0) {
                            result = treeMap.get(key) +  "," + result;
                        } else {
                            result = "" + treeMap.get(key);
                        }
                    }
                }
                allAllelesValues[i] = "" + result;
            }

        }
    }

    /**
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * 
     * Steps: Create a task and add it to both pools and then start it immediately.
     *        Split work evenly by giving tasks start and end lines
     * <p>
     * Results are written to a .csv text file at the set file location. The result files are meant to be merged sequentially
     * after all threads have completed.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    @Deprecated
    private void convertHmpToCsvThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new HmpToCsvTask[workers];
            Thread[] threadPool = new Thread[workers];
            for (int i = 0; i < workers; ++i) {
                resultsPool[i] = new HmpToCsvTask(
                    outputFileName + TEMP_FILE_NAME + i,
                    outputFileName + TEMP_FILE_NAME_2ND + i,
                    (i * (totalInputColumns - NUMBER_OF_HEADER_COLUMNS) / workers),
                    ((1 + i) * (totalInputColumns - NUMBER_OF_HEADER_COLUMNS) / workers),
                    totalInputColumns - NUMBER_OF_HEADER_COLUMNS,
                    totalInputLines - NUMBER_OF_HEADER_LINES,
                    majorAllelesValues,
                    hmpPloidiness
                );
                threadPool[i] = new Thread(resultsPool[i]);
                threadPool[i].start();
            }
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("Error joining results worker [" + i + "].");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * 
     * Steps: Create a task and add it to both pools and then start it immediately.
     *        Split work evenly by giving tasks start and end lines
     * <p>
     * Results are written to a .csv text file at the set file location. The result files are meant to be merged sequentially
     * after all threads have completed.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     */
    private void convertHmpToCsvLargeThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new HmpToCsvTaskLarge[workers];
            Thread[] threadPool = new Thread[workers];
            for (int i = 0; i < workers; ++i) {
                resultsPool[i] = new HmpToCsvTaskLarge(
                    outputFileName + TEMP_FILE_NAME + i,
                    outputFileName + TEMP_FILE_NAME_2ND + i,
                    (i * (totalInputLines - NUMBER_OF_HEADER_LINES) / workers), // start line
                    (((1 + i) * (totalInputLines - NUMBER_OF_HEADER_LINES)) / workers), // end line
                    majorAllelesValues, // majorAlleles
                    i // the portion of the total data it will work on.
                );
                threadPool[i] = new Thread(resultsPool[i]);
                threadPool[i].start();
            }
            for (int i = 0; i < workers; ++i) {
                try {
                    threadPool[i].join();
                } catch (InterruptedException e) {
                    System.out.println("Error joining results worker [" + i + "].");
                    e.printStackTrace();
                }
            }

            // Check that the workers completed tasks and merge their results:
            for (int i = 0; i < workers; ++i) {
                int pieces = ((HmpToCsvTaskLarge)resultsPool[i]).getNumberOfFilesInSeries();
                try {
                    FileController.mergeFilePieces(
                        (totalInputColumns - NUMBER_OF_HEADER_COLUMNS),
                        pieces, // Number of files in the series.
                        outputFileName + TEMP_FILE_NAME_2ND, // result file
                        i // the portion/worker number
                    );
                } catch (DiskFullException e) {
                    System.out.println("Error: The disk appears to be full. However, partial results are available.");
                }
            }
        }
    }

    /**
     * All access to totals must be synchronized. Including reading of data through .get().
     * 
     * @return  The synchronized totals datastructure.
     */
    public synchronized List<int[]> getTotals() {
        return totals;
    }

    /**
     * Initializes and fills the outputLineHeaders data structure. Tab separators are included in the
     * entry strings. The first entry will include the file header and each line will also include
     * its line header.
     */
    private void createOutputLineHeaders() {
        outputLineHeaders = new String[totalInputLines - NUMBER_OF_HEADER_LINES];
        for (int i = 0; i < outputLineHeaders.length; ++i) {
            outputLineHeaders[i] = "";
        }

        String version = "##fileformat=VCFv4.2";

        String format = "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">";

        String columnHeaders = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";

        outputLineHeaders[0] = version + "\n" + format + "\n" + columnHeaders;

        for (int i = 0; i < sampleIds.length; ++i) {
            outputLineHeaders[0] += ("" + "\t" + sampleIds[i]);
        }
        outputLineHeaders[0] += ("" + "\n");
        // Done making the file header.
        // Add the line header to each entry outputLineHeaders:
        for (int i = 0; i < outputLineHeaders.length; ++i) {
            //String alleleOne = majorAllelesValues[i];
            String allAlleles = allAllelesValues[i];
            String result = "";

            //result = "" + alleleOne + '\t' + alleleTwo + '\t';

            // If alleleTwo only has 1 entry, then it is the major,
            // otherwise, split the array on comma and place a tab-char in-between
            // the first and second entries to create the results string.
            if (allAlleles.length() == 1) {
                result = "" + allAlleles + "\t.\t";
            } else {
                String[] values = allAlleles.split(",");
                result = "" + values[0] + "\t"; 
                for (int j = 1; j < values.length; ++j) {
                    if (j == values.length - 1) {
                        result += ("" + values[j]);
                    } else {
                        result += ("" + values[j] + ",");
                    }
                }
                result += "\t";
            }

            // Construct line header string:
            outputLineHeaders[i]
            // HMP:
            //rs#	alleles	chrom	pos	strand	assembly#	center	protLSID	assayLSID	panel	QCcode  ...
            // VCF:
            //#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	...
                += (""
                + getLineHeaders().get(i)[2] + "\t" // #CHOM
                + getLineHeaders().get(i)[3] + "\t" // POS
                + getLineHeaders().get(i)[0] + "\t" // ID
                + result // REF + ALT
                + ".\t" // QUAL -> .
                + "NA\t" // FILTER -> NA/PASS
                + ".\t" // INFO -> .
                + "GT\t"); // FORMAT
        }
    }

    /**
     * Merges the set of files into a single file at the provided path and name.
     * 
     * @param count The number of files in the set, from 0 to count - 1, inclusive.
     * @param resultFile    The output file name with path and with an extension.
     * @param tempName  The intermediate file containing its appendix, file path, and an extension.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     * 
     * Note: Will overwrite existing data with no warning or prompts.
     */
    private void mergeFiles(int fileCount, String resultFile, String tempName) throws DiskFullException {
        FileController.mergeFiles(fileCount, resultFile, tempName);
    }

    /**
     * Attempts to clean up all of the temporary files.
     * 
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    private void cleanUpAll() {
        FileController.cleanUp(NUMBER_OF_WORKERS, outputFileName, TEMP_FILE_NAME); // Delete stage 1 files.
        if (FileController.canReadFile(outputFileName + TEMP_FILE_NAME)) {
            FileController.deleteSingleFile(outputFileName + TEMP_FILE_NAME); // Delete combined stage 1 result.
        }
        FileController.cleanUp(NUMBER_OF_WORKERS, outputFileName, TEMP_FILE_NAME_2ND); // Delete stage 2 files.
        FileController.deleteTempFolders(NUMBER_OF_WORKERS, outputFileName);
    }

    // Debug helper:
    private void printThreadTotals() {
        //for (HmpSumTask task : hmpSumPool) {
        for (int k = 0; k < sumPool.length; ++ k) {
            for (int i = 0; i < sumPool[k].getTotals().size(); ++i) { // 773/774.
                System.out.print("(" + i + ") ");
                int sum = 0;
                for (int j = 0; j < sumPool[k].getTotals().get(i).length; ++j) {
                    System.out.print(" " + sumPool[k].getTotals().get(i)[j]);
                    sum += sumPool[k].getTotals().get(i)[j];
                }
                System.out.println(" " + sum);
                if (sum != 2 * (totalInputColumns - NUMBER_OF_HEADER_COLUMNS)) {
                    System.out.println("Invalid sum. Results are not accurate: " + sum);
                }
            }
        }
    }

    // Prints and checks integrity of results:
    // Debug helper:
    private void printIntermediateData() {
        for (int i = 0; i < majorAllelesValues.length; ++i) {
            System.out.print("[" + i + "] " + majorAllelesValues[i]);
            int sum = 0;
            for (int j = 0; j < getTotals().get(i).length; ++j) {
                System.out.print(" " + getTotals().get(i)[j]);
                sum += getTotals().get(i)[j];
            }
            System.out.println(" " + sum);
        }
    }

    // Debug helper
    private void printDebugLineInfo() {
        System.out.println("Converting file: [" + hmpFileName + "] to [" + outputFileName + "]");
        System.out.println("Lines: " + totalInputLines);
        System.out.println("Input columns: " + totalInputColumns);
        System.out.println("Header lines: " + NUMBER_OF_HEADER_LINES);
        System.out.println("Header line length: " + NUMBER_OF_HEADER_COLUMNS);
    }

    // Debug helper - print all sample ids
    private void printSampleIds() {
        for (int i = 0; i < sampleIds.length; ++i) {
            System.out.println("sampleIds[" + i + "]: " + sampleIds[i]);
        }
    }

    // Debug helper - print all strand directions
    private void printStrandDirections() {
        for (int i = 0; i < strandDirections.length; ++i) {
            System.out.println("strandDirections[" + i + "]: " + strandDirections[i]);
        }
    }

    // Debug helper - prints all Alleles Values.
    private void printAllAllelesValues() {
        for (int i = 0; i < allAllelesValues.length; ++i) {
            System.out.println("allAllelesValues[" + i + "]: [" + allAllelesValues[i] + "]");
        }
    }

    // Debug helper - prints all headers.
    private void printOutputLineHeaders() {
        for (int i = 0; i < outputLineHeaders.length; ++i) { //outputLineHeaders.length
            System.out.println("[" + i + "]: [" + outputLineHeaders[i] + "]");
        }
    }

    /**
     * Returns the input file path with an extension.
     *
     * @return  The input file name and path with an extension.
     */
    public String getInputFilePath() {
        return hmpFileName;
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
