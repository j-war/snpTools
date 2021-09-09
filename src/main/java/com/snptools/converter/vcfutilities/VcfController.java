package com.snptools.converter.vcfutilities;

import com.snptools.converter.fileutilities.DiskFullException;
import com.snptools.converter.fileutilities.FileController;
import com.snptools.converter.fileutilities.NormalizeInputTask;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * The VcfController class directs the conversion of a .vcf text file into a .csv or .hmp output file.
 * @author  Jeff Warner
 * @version 1.1, June 2021
 */
public class VcfController {

    private volatile String vcfFileName = "./input.vcf"; // The input file name with path and extension.
    private volatile String outputFileName = "./output.csv"; // The output file name with path and extension.
    private final String TEMP_FILE_NAME = "TEMP"; // File name appendix for stage 1.
    private final String TEMP_FILE_NAME_2ND = "TEMP_2ND"; // File name appendix for stage 2.
    private final int NUMBER_OF_WORKERS = 4; // The number of worker threads to create.

    final int MAJOR_REF_ALLELE_POSITION = 3; // The position of the major allele.
    final int ALT_ALLELE_POSITION = 4; // The position of a comma seperated String of all SNPs for this site.

    private int totalInputLines = 0; // The number of lines in the file, # of samples = totalInputLines - numberOfHeaderLines.
    private int numberOfHeaderLines = 0; // The size of the file header.
    private int inputColumnCount = 0; // 290. The number of allele pairs = inputColumnCount - numberOfHeaderColumns
    private int numberOfHeaderColumns = 8; // Default of 8, optionally 9 if formatColumnPresent is true.
    private boolean formatColumnPresent = false;
    private List<String[]> lineHeaders = Collections.synchronizedList(new ArrayList<>());
    private volatile String[] alleles; // The csv string of alleles from the vcf line headers.
    private volatile String[] sampleIds; // The list of ids from the file header.
    private volatile String[] outputLineHeaders; // The constructed line headers.

    private volatile NormalizeInputTask[] normalizePool; // The pool to hold the pre-processing workers.
    private volatile Runnable[] resultsPool; // The pool to hold the conversion workers.

    /**
     * Constructor for the VcfController class - can be used to read and convert .vcf files.
     * @param inputName The input file name with path and extension.
     * @param outputName    The output file name with path and extension.
     */
    public VcfController(String inputName, String outputName) {
        this.vcfFileName = inputName;
        this.outputFileName = outputName;
    }

    public void startVcfToCsv() {
        totalInputLines = FileController.countTotalLines(vcfFileName); // 125ms. 800ms on 300mb, [1.3gb: 3900-5500ms on 150 lines, 8.5million character line, 4.27m columns].
        numberOfHeaderLines = countHeaderLines();
        inputColumnCount = countLineLength(); // 70ms. [1.3gb: ~1500 on 150 lines, 8.5million character line, 4.27m columns]

        // Simple integrity check:
        if (totalInputLines <= 0 || numberOfHeaderLines <= 0 || inputColumnCount <= 0) {
            System.out.println("\nThe file header is missing. Could not detect column titles. Please make sure the vcf file is valid.\n");
            return;
        }
        if (numberOfHeaderColumns < 8 || numberOfHeaderColumns > 9) {
            System.out.println("\nThe header column count appears in valid. Please make sure the vcf file is valid.\n");
            return;
        }
        // determinePloidiness() // NYI.

        printDebugLineInfo();
        // Simple integrity check:
        if (numberOfHeaderLines <= 0 || totalInputLines <= 0 || numberOfHeaderLines == totalInputLines) {
            System.out.println("\nThe file header is missing or could not detect column titles. Please make sure the vcf file is valid.\n");
            return;
        }

        normalizeInputThreaded(NUMBER_OF_WORKERS); // Strip and normalize genotype data out and into an intermediate file.
        try {
            String intermediateFile = outputFileName + TEMP_FILE_NAME;
            mergeFiles(NUMBER_OF_WORKERS, intermediateFile, outputFileName + TEMP_FILE_NAME);
            // total: 800-900ms for 3.5mb
    
            processInputThreaded(NUMBER_OF_WORKERS); // Process the prepared files to their results.
            mergeFiles(NUMBER_OF_WORKERS, outputFileName, outputFileName + TEMP_FILE_NAME_2ND);

            cleanUpAll();
        } catch (DiskFullException e) {
            System.out.println("Error: An IOException occurred - the disk may be full.");
            System.out.println("\nWarning: Partial results are available but not may not be valid.\n");
            e.printStackTrace();
        }
    }

    public void startVcfToHmp() {
        totalInputLines = FileController.countTotalLines(vcfFileName); // 125ms. 800ms on 300mb, [1.3gb: 3900-5500ms on 150 lines, 8.5million character line, 4.27m columns].
        numberOfHeaderLines = countHeaderLines();
        inputColumnCount = countLineLength(); // 70ms. [1.3gb: ~1500 on 150 lines, 8.5million character line, 4.27m columns]

        // determinePloidiness() // NYI.

        printDebugLineInfo();
        // Simple integrity check:
        if (numberOfHeaderLines <= 0 || totalInputLines <= 0 || numberOfHeaderLines == totalInputLines) {
            System.out.println("\nThe file header is missing. Could not detect column titles. Please make sure the vcf file is valid.\n");
            return;
        }

        initLineHeaders();
        collectLineHeaders();
        //printDebugLineInfo();
        //printIntermediateData();

        collectAlleles();
        //printAlleles();
        collectSampleIds();
        if (sampleIds == null || (sampleIds.length != (inputColumnCount - numberOfHeaderColumns))) {
            System.out.println("\nSample ids could not be detected. Please make sure the vcf file is well formed and that the number of records matches the number of ids.\n");
            return;
        }
        createOutputLineHeaders();

        normalizeInputThreaded(NUMBER_OF_WORKERS); // Strip and normalize genotype data out and into an intermediate file.
        try {
            String intermediateFile = outputFileName + TEMP_FILE_NAME;
            mergeFiles(NUMBER_OF_WORKERS, intermediateFile, outputFileName + TEMP_FILE_NAME);
            // total: 800-900ms for 3.5mb
    
            convertVcfToHmpThreaded(NUMBER_OF_WORKERS);
            mergeFiles(NUMBER_OF_WORKERS, outputFileName, outputFileName + TEMP_FILE_NAME_2ND);
    
            cleanUpAll();
        } catch (DiskFullException e) {
            System.out.println("Error: An IOException occurred - the disk may be full.");
            System.out.println("\nWarning: Partial results are available but not may not be valid.\n");
            e.printStackTrace();
        }
    }

    /**
     * Counts the number of header lines by checking for lines with a '#' character at the
     * start of a line.
     * This method assumes the file header is well formed and all meta data lines are at the
     * start of the file and have no breaks or empty lines in between them. It is finished
     * counting when a '#' is not found, an error occurs, or it reaches the end of the file.
     *
     * @return  The number of sequential lines from the start of the file that were detected.
     *          Returns 0 on any error.
     */
    private int countHeaderLines() {
        try (BufferedReader reader = new BufferedReader(new FileReader(vcfFileName))) {
            int lines = 0;
            for (int i = 0; i < totalInputLines; ++i) {
                String line = reader.readLine();
                //System.out.println("Line: [" + line + "]");
                if (line != null) {
                    Scanner lineScanner = new Scanner(line);
                    if (lineScanner.hasNext()) {
                        String token = lineScanner.next();
                        if (token.startsWith("#")) { // "#CHROM"
                            //System.out.println("# found: " + token);
                            ++lines;
                        } else {
                            lineScanner.close();
                            return lines; // Exit early.
                        }
                    } // else, fall through.
                    lineScanner.close();
                } else {
                    System.out.println("The read line was null when checking the file header.");
                    return 0;
                }
            }
            //numberOfHeaderLines = lines; // Save the total.
            return 0; // The early exit path should be the only with non-zero - this means that path detected "#CHROM".
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found or it could not be opened.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the file.");
            e.printStackTrace();
            return 0;
        }
    }
    
    /**
     * Counts the number of columns present in the vcf file. The number of data
     * input columns is returned while the number of header columns is set manually.
     * The presence of a "FORMAT" column in the header is checked and set as well.
     * 
     * Also, determines and sets whether the optional "FORMAT" column is present.
     * 
     * @return  The total number of columns that were detected.
     *          Returns 0 on an error - side effects listed below may still occur.
     * 
     * Note: This method has two side effects of altering the variables 
     *       formatColumnPresent and numberOfHeaderColumns.
     */
    private int countLineLength() {
        try (BufferedReader reader = new BufferedReader(new FileReader(vcfFileName))) {
            int lineLength = 0;
            for (int i = 0; i < numberOfHeaderLines - 1; ++i) { reader.readLine(); } // Skip ahead.
            String line = reader.readLine();
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                String token = "";
                while (lineScanner.hasNext()) {
                    token = lineScanner.next();
                    ++lineLength;
                    //System.out.println("Token: " + token);
                    if (token.equalsIgnoreCase("FORMAT")) {
                        //System.out.println("      Format found." + lineLength);
                        formatColumnPresent = true;
                        numberOfHeaderColumns = lineLength; // = 9.
                    }
                }
                lineScanner.close();
                return lineLength;
            } else {
                System.out.println("The read line was null when checking the file header.");
                return 0;
            }
            //inputColumnCount = lineLength; // Save the total.
            //return lineLength;
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found or it could not be opened.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Assumes the vcf input file is well formed.
     * Retrieves and saves the line headers of the set input file to the synchronized
     * lineHeaders data structure.
     * The file header meta data is skipped over and the subsequent lines are collected.
     */
    private void collectLineHeaders() {
        try (BufferedReader reader = new BufferedReader(new FileReader(vcfFileName))) {
            for (int i = 0; i < numberOfHeaderLines; ++i) { reader.readLine(); } // Skip ahead.
            for (int i = 0; i < totalInputLines - numberOfHeaderLines; ++i) {
                String line = reader.readLine();
                //System.out.println("Line: [" + line + "]");
                if (line != null) {
                    Scanner lineScanner = new Scanner(line);
                    for (int j = 0; j < numberOfHeaderColumns; ++j) {
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
                } else {
                    System.out.println("The read line was null when checking the line headers.");
                    return;
                }
            }
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Missing or unexpected data while collecting line headers.");
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found or it could not be opened.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an IO error checking the file.");
            e.printStackTrace();
        }
    }

    /**
     * Initializes and fills the synchronized lineHeaders datastructure with default data.
     */
    private void initLineHeaders() {
        // Populate the lineHeaders to store sample/entry data.
        synchronized (lineHeaders) {
            for (int i = 0; i < totalInputLines - numberOfHeaderLines; ++i) {
                lineHeaders.add(i, new String[numberOfHeaderColumns]);
            }
        }
    }

    /**
     * Initializes and fills the alleles array with data from the synchronized lineHeaders data structure.
     * The contained string is a csv string of the major allele with the reference alleles appended
     * to it as they appear in the .vcf file line headers.
     */
    private void collectAlleles() {
        // Add a single string containing all SNPs with a comma separator in between.
        alleles = new String[totalInputLines - numberOfHeaderLines];
        for (int i = 0; i < alleles.length; ++i) {
            alleles[i] = "" + getLineHeaders().get(i)[MAJOR_REF_ALLELE_POSITION] + "," + getLineHeaders().get(i)[ALT_ALLELE_POSITION];
        }
    }

    /*
     * Initializes and fills a string array of sample ids from the input vcf file.
     * The scanner skips over the file header and saves the string tokens to the
     * sampleIds array.
     */
    private void collectSampleIds() {
        try (BufferedReader reader = new BufferedReader(new FileReader(vcfFileName))) {
            sampleIds = new String[inputColumnCount - numberOfHeaderColumns];
            for (int i = 0; i < numberOfHeaderLines - 1; ++i) { reader.readLine(); } // Skip ahead.
            String line = reader.readLine(); // Should be the line containing sample ids - starts with "#CHROM".
            if (line != null) {
                Scanner lineScanner = new Scanner(line);
                for (int i = 0; i < numberOfHeaderColumns; ++i) { // Skip ahead to the sample ids.
                    if (lineScanner.hasNext()) {
                        lineScanner.next();
                    } else {
                        System.out.println("Malformed header detected when collecting sample ids.");
                        lineScanner.close();
                        return;
                    }
                }
                for (int i = 0; i < inputColumnCount - numberOfHeaderColumns; ++i) {
                    if (lineScanner.hasNext()) {
                        sampleIds[i] = "" + lineScanner.next();
                        //System.out.println("Sample ids [" + i + "]: " + sampleIds[i]);
                    } else {
                        System.out.println("Malformed number of sample ids detected.");
                        lineScanner.close();
                        return;
                    }
                }
                lineScanner.close();
            } else {
                System.out.println("The read line was null when collecting the sample ids.");
                return;
            }
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found or it could not be opened.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an IO error checking the file.");
            e.printStackTrace();
        }
    }

    /**
     * Creates HMP file output file and line headers from the synchronized lineHeaders
     * data structure.
     * 
     * Initializes and fills the output file's line header data structure and
     * saves the results into the outputLineHeaders data structure.
     * 
     * Note: Since, strand direction information is not included in a VCF file we must
     *       then provide consistent behaviour when converting from VCF to HMP. We chose to
     *       output the alleles data column in the HMP file in alphabetical order of
     *       the possible/reported SNPs for that site. This has the impact of possible
     *       rearrangment of the data in the HMP output file but provides a systematic
     *       and predictable output.
     */
    private void createOutputLineHeaders() {
/* Example: file header + newline + first data entries line header

outputLineHeaders[0] =
        (column headers) + (sample ids) + (\n) = 
        "rs#	alleles	chrom	pos	strand	assembly#	center	protLSID	assayLSID	panel	QCcode" + "ids" + "\n"

outputLineHeaders[1] onwards = 
        vcf[2] + (vcf[3] + "/" + vcf[4]) + vcf[0] + vcf[1] + "." + "." + "NA" + "NA" + "NA" + NA" + "NA" + (data here)

        where (data here) is filled in in the conversion task worker.
*/

        outputLineHeaders = new String[totalInputLines - numberOfHeaderLines];
        for (int i = 0; i < outputLineHeaders.length; ++i) {
            outputLineHeaders[i] = "";
        }
        String columnHeaders = "rs#\talleles\tchrom\tpos\tstrand\tassembly#\tcenter\tprotLSID\tassayLSID\tpanel\tQCcode";
        outputLineHeaders[0] = columnHeaders;

        for (int i = 0; i < sampleIds.length; ++i) {
            outputLineHeaders[0] += ("" + '\t' + sampleIds[i]);
        }
        outputLineHeaders[0] += ("" + '\n');


        for (int i = 0; i < outputLineHeaders.length; ++i) {
            String alleleOne = (getLineHeaders().get(i)[MAJOR_REF_ALLELE_POSITION]).substring(0, 1);
            String alleleTwo = (getLineHeaders().get(i)[ALT_ALLELE_POSITION]).substring(0, 1);
            String result = "";

            // If either entry is a '.', then the data is missing and should be
            // altered to be an 'N' for the HMP file format.
            if (alleleOne.compareToIgnoreCase(".") == 0) {
                alleleOne = "N";
            }
            if (alleleTwo.compareToIgnoreCase(".") == 0) {
                alleleTwo = "N";
            }

            // Set the result output to be alphabetical:
            if (alleleOne.compareToIgnoreCase(alleleTwo) < 0) {
                // alleleOne is smaller.
                result = "" + alleleOne + "/" + alleleTwo;
            } else if (alleleOne.compareTo(alleleTwo) > 0)  {
                // alleleOne is greater
                result = "" + alleleTwo + "/" + alleleOne;
            } else {
                // Equal.
                result = "" + alleleOne + "/" + alleleTwo;
            }
            // Construct line header string:
            // HMP:
            //rs#	alleles	chrom	pos	strand	assembly#	center	protLSID	assayLSID	panel	QCcode  ...
            // VCF:
            //#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	...
            outputLineHeaders[i]
                += (""
                + getLineHeaders().get(i)[2] + '\t' // rs#
                + result + '\t' // alleles
                + getLineHeaders().get(i)[0] + '\t' // chrom
                + getLineHeaders().get(i)[1] + '\t' // pos
                + ".\t.\tNA\tNA\tNA\tNA\tNA\t"); // strand,assembly#,center,protLSID,assayLSID,panel,QCcode,[data].
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
     * Normalizes the data input file for uniform processing and file navigation and improved performance.
     * <p>
     * File headers and line headers are stripped according to previously determined constraints. 
     * Work is distributed evenly amongst worker threads.
     * <p>
     * Only the genotype (GT) data is taken from the data entries.
     * 
     * @param workers   The number of workers for this task. Simply the number of threads to create.
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
/* Example:
    i = 0
    (i * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 0 + numberOfHeaderLines = 11
    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = (totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = (3104-11)/4 + 11 = 784.25

    i = 1
    (i * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 1*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 1*(3104-11)/4 + 11 = 784.25
    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 2*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 2*(3104-11)/4 + 11 = 1557.5

    i = 2
    (i * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 2*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 2*(3104-11)/4 + 11 = 1557.5
    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 3*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 3*(3104-11)/4 + 11 = 2330.75

    i = 3
    (i * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 3*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 3*(3104-11)/4 + 11 = 2330.75
    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                        = 4*(totalInputLines - numberOfHeaderLines)/4 + numberOfHeaderLines = 4*(3104-11)/4 + 11 = 3104

*/
                normalizePool[i] = new NormalizeInputTask(
                    vcfFileName,
                    outputFileName + TEMP_FILE_NAME + i,
                    (i * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers) + numberOfHeaderLines,
                    numberOfHeaderColumns,
                    inputColumnCount - numberOfHeaderColumns,
                    3 // Column width: the number of chars.
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
     * <p>
     * Results are written to a .csv text file at the set file location. The result files are meant
     * to be merged sequentially after all threads have completed.
     *
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     *
     * Note: Only diploid cells are currently supported.
     */
    private void processInputThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new VcfToCsvTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and start it immediately.
            // Split work evenly:
            for (int i = 0; i < workers; ++i) {
                resultsPool[i] = new VcfToCsvTask(
                    outputFileName + TEMP_FILE_NAME,
                    outputFileName + TEMP_FILE_NAME_2ND + i,
                    (i * (inputColumnCount - numberOfHeaderColumns) / workers),
                    ((1 + i) * (inputColumnCount - numberOfHeaderColumns) / workers),
                    inputColumnCount - numberOfHeaderColumns,
                    totalInputLines - numberOfHeaderLines
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
     * Process the intermediate data that has been normalized.
     * Creates a pool of workers and distributes the work evenly to calculate the results.
     * <p>
     * Results are written to a .csv text file at the set file location. The result files are meant to be merged sequentially
     * after all threads have completed.
     *
     * @param workers   The number of workers for this task. Simply the number of threads to create.
     *
     * Note: Only diploid cells are currently supported.
     */
    private void convertVcfToHmpThreaded(int workers) {
        if (workers > 0) {
            resultsPool = new VcfToHmpTask[workers];
            Thread[] threadPool = new Thread[workers];
            // Create task and add it to both pools and start it immediately.
            // Split work evenly:
            for (int i = 0; i < workers; ++i) {
                // Input file
                // Output file
                // Line to start
                // End line
                // How many columns
                // Alleles[]

                resultsPool[i] = new VcfToHmpTask(
                    outputFileName + TEMP_FILE_NAME,
                    outputFileName + TEMP_FILE_NAME_2ND + i,
                    (i * (totalInputLines - numberOfHeaderLines) / workers),
                    ((1 + i) * (totalInputLines - numberOfHeaderLines) / workers),
                    inputColumnCount - numberOfHeaderColumns,
                    alleles,
                    outputLineHeaders
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
     * Merges the set of files into a single file at the provided path and name.
     * 
     * @param count The number of files in the set, from 0 to count - 1, inclusive.
     * @param resultFile    The output file name with path and with an extension.
     * @param tempName  The intermediate file containing its appendix, file path, with an extension.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     * 
     * Note: Will overwrite existing data with no warning or prompts.
     */
    private void mergeFiles(int count, String resultFile, String tempName) throws DiskFullException {
        FileController.mergeFiles(count, resultFile, tempName);
    }

    /**
     * Attempts to clean up all of the temporary files.
     *
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    private void cleanUpAll() {
        FileController.cleanUp(NUMBER_OF_WORKERS, outputFileName, TEMP_FILE_NAME); // Delete stage 1 files.
        FileController.deleteSingleFile(outputFileName + TEMP_FILE_NAME); // Delete combined stage 1 result.
        FileController.cleanUp(NUMBER_OF_WORKERS, outputFileName, TEMP_FILE_NAME_2ND); // Delete stage 2 files.
    }

    // Debug helper
    private void printDebugLineInfo() {
        System.out.println("Converting file: [" + vcfFileName + "] to [" + outputFileName + "]");
        System.out.println("Lines: " + totalInputLines);
        System.out.println("Header lines: " + numberOfHeaderLines);
        System.out.println("Header line length: " + numberOfHeaderColumns);
        System.out.println("Line length: " + inputColumnCount);
        System.out.println("Format header present: " + formatColumnPresent);
    }

    // Prints and checks integrity of results:
    // Debug helper:
    private void printIntermediateData() {
        for (int i = 0; i < totalInputLines - numberOfHeaderLines; ++i) {
            System.out.print("[" + i + "] ");
            String line = "";
            for (int j = 0; j < getLineHeaders().get(i).length; ++j) {
                System.out.print(" " + getLineHeaders().get(i)[j]);
                line += getLineHeaders().get(i)[j];
            }
            System.out.println("");
            System.out.println(" " + line);
        }
    }

    // Debug helper:
    private void printAlleles() {
        for (int i = 0; i < 440; ++i) { //alleles.length
            System.out.println("[" + i + "] ");
            System.out.println("[" + alleles[i] + "]");
        }
    }

    /**
     * Returns the input file path with an extension.
     *
     * @return  The input file name and path with an extension.
     */
    public String getInputFilePath() {
        return vcfFileName;
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
