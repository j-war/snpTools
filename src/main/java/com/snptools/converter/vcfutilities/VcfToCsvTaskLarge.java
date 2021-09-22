package com.snptools.converter.vcfutilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The VcfToCsvTaskLarge class is used to transpose and translate vcf data to csv.
 * <p>
 * The resulting files are to be merged sequentially, by line, once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, September 2021
 */
public class VcfToCsvTaskLarge implements Runnable {

    private final int BAD_DATA = 9; // The SNP at this site is missing from the vcf file - there was no data for this site. Note: the output csv file may have a 4 or 5 for this site. 4 if both alleles were missing data, or 5 if only 1 allele was.
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.

    private final long startLine;
    private final long endLine;

    private final int portion; // The portion of the total data that this worker is working on.

    volatile private int numberOfFilesInSeries = 0; // Value to be retrieved for merging after this task completes.

    private static final int VCF_HAPLOID_WIDTH = 1;
    private static final int VCF_DIPLOID_WIDTH = 3; // The number of characters wide including the inner separator.
    private static final int VCF_TRIPLOID_WIDTH = 5;
    private static final int MISSING_DATA = 5; // CSV format uses 5 for two missing data points in diploids.
    //private static final long SIZE_OF_VCF_COLUMN = 4;

    /**
     * Uses characteristics of the VCF input file to determine an output and writes it to the provided path.
     * This conversion involves a transpose of VCF columns into CSV lines.
     * The startColumn and endColumn arguments are used for file pointer positioning while the
     * totalColumns and totalLines arguments are used for transposing and transforming the data.
     * 
     * @param inputFilename The input file path and file name and an extension for processing.
     * @param outputFilename    The  The output file path and file name with an extension for processing.
     * @param startLine   The start line for this worker to start at.
     * @param endLine The end line for this worker to finish at.
     * @param portion    The worker number assigned to this chunk of the output.
     */
    public VcfToCsvTaskLarge(String inputFilename, String outputFilename, int startLine, int endLine, int portion) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = (long) startLine;
        this.endLine = (long) endLine;
        this.portion = portion;
    }

    @Override
    public void run() {
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
        ) {
            long X = startLine;
            final long Y = endLine;
            int chunkSize = 512; // Tuning parameter: 1500+ with 4 workers and 8gb will likely throw OOM, ~250 was slower that 500.

            // Create temp folder - assume we will need folders:
            Path outputFolder = Paths.get(outputFilename).getParent().normalize();
            Path outputFile = Paths.get(outputFilename).getFileName();
            File tempDir = new File(outputFolder.toString() + "/temp" + portion + "/");
            if (!tempDir.exists()) {
                tempDir.mkdir();
            } // else { System.out.println("Temp directory already exists."); }

            while (X < Y) {
                List<List<Integer>> inputChunk = new ArrayList<List<Integer>>(); // Inner List holds lines/rows.
                List<List<Integer>> outputChunk = new ArrayList<List<Integer>>(); // Inner List holds columns.

                for (int j = 0; j < chunkSize; ++j) {
                    inputChunk.add(j, new ArrayList<Integer>());
                    String line = reader.readLine();
                    if (line != null) {
                        if (!(line.isBlank()) || !(line.isEmpty())) {
                            String[] arr = line.split(",");

                            for (String entry : arr) {
                                //(inputChunk.get(j)).add(accumulateResults(X + j, entry)); // Deprecated.
                                (inputChunk.get(j)).add(interpretEntry(entry));
                            }
                        } // else { // Line is blank/newline, ignore it by incrementing offset: }
                    } // else { // total lines not divisible by chunksize - we read a null, increment offset and continue. }
                } // Done reading in a chunk of input.

                // Transpose the processed lines to output lists:
                // The length/size of the first line is the number of lines in the output:
                // Initialize the output's inner list:
                for (int j = 0; j < inputChunk.get(0).size(); ++j) { // For the number of input columns, add an output row:
                    outputChunk.add(j, new ArrayList<Integer>());
                }

                // Transpose inputChunk to outputChunk:
                for (int j = 0; j < inputChunk.size(); ++j) { // for the number of input rows.
                    for (int k = 0; k < (inputChunk.get(j)).size(); ++k) { // for the number of entries on a line (input columns)
                        (outputChunk.get(k)) .add(j, (inputChunk.get(j)).get(k)); // Do NOT catch potential OOM.
                    }
                }

                // Pass outputChunk to accumulator(): pass in the column (old method passed line, then transposed. We transpose then translate, so use the column and the value!)

                // Write outputChunk:
                FileOutputStream outputChunkStream = new FileOutputStream("./" + tempDir + "/" + outputFile + numberOfFilesInSeries);
                OutputStreamWriter outputChunkWriter = new OutputStreamWriter(outputChunkStream);
                String result = "";
                for (int j = 0; j < outputChunk.size(); ++j) {
                    if (X == 0 && portion == 0) { // Only the first file in the series should get a phenotype.
                        result = "-9,"; // Phenotype placeholder.
                    } else {
                        result = "";
                    }
                    for (int k = 0; k < outputChunk.get(j).size(); ++k) {
                        if (k < outputChunk.get(j).size() - 1) {
                            result += ((outputChunk.get(j)).get(k) + ",");
                        } else {
                            result += ((outputChunk.get(j)).get(k) + "\n");
                        }
                    }
                    outputChunkWriter.write(result);
                }
                outputChunkWriter.close();

                ++numberOfFilesInSeries;

                X += chunkSize;
            }
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file in a VCFToCsvTaskLarge worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the intermediate file.");
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Index does not match expectation. Possible malformed VCF file.");
            e.printStackTrace();
        }
    }

    /**
    * Interprets the provided string that was read from the normalized data file
    * and returns the csv data value. Outputs a 9 if the input file contains
    * malformed entries.
    * 
    * @param entry The data entry to be converted from VCF to CSV
    * @return   The result of the interpretation.
    *       
    * Note: This is best used with haploid and diploid cells. It will work with triploid
    *       cells but the encoding results will have a 3 in the case of 3 minors...
    */
    private int interpretEntry(String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            //partialResults[lineNumber] = NO_DATA_DIPLOID_IN_CSV;
            return MISSING_DATA;
        }
        /*
        SNP coding: for diploid,
        If we have two major alleles, it is 0.
        One major and one minor, it is 1.
        Two minor alleles, it is 2.
        */
        int result = 0;
        int parsedValue = 0;
        // Compare allele to majorAllelesValues array to determine output:
        for (int k = 0; k < entry.length(); ++k) {
            String allele = entry.substring(k, 1 + k);
            switch (allele) {
                case "/", "|":
                    // Do nothing, skip.
                    break;
                case ".", "-", "*":
                    result = MISSING_DATA; // 5.
                    break;
                default:
                if (result != BAD_DATA) {
                    try {
                        parsedValue = Integer.parseInt(allele);
                        if (parsedValue > 0) {
                            ++result;
                        }
                    } catch (NumberFormatException e) {
                        result = BAD_DATA;
                        // break; // Don't continue checking the rest of the entry.
                    }
                }
                break;
            }
        }
        // Save result to linebuffer:
        //partialResults[lineNumber] = result;
        return result;
    }

    /**
    * Interprets the provided string that was read from the normalized data file
    * and stores the results in the line buffer, partialResults.
    * 
    * @param lineNumber    The line number that is being parsed.
    * @param entry The data entry to be converted from VCF to CSV
    * Note: Does not cover the case of a single missing value in diploid cells
    *       Example: "./#" or "#/." etc.
    *       
    *       If the entry is a bad length then the output will be 5/missing data.
    */
    @Deprecated
    private void accumulateResults(long lineNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            //partialResults[lineNumber] = NO_DATA_DIPLOID_IN_CSV;
            //return (MISSING_DATA + 1);
        }
        // Check if error ./.
        // Check if 2 zeros.
        // Check if 1 zero
        //   else, two minor alleles.
        /*
        SNP coding: for diploid,
        If we have two major alleles, it is 0.
        One major and one minor, it is 1.
        Two minor alleles, it is 2.
        */
        int result = MISSING_DATA;
        switch (entry) {
            case "0/0", "0|0": // 2 Majors.
            result = 0;
            break;
            case "./.", ".|.", "*/*", "*|*": // Missing data.
            result = MISSING_DATA;
            break;

            default: // Currently only valid for diploids and assumes data entries are well-formed:
            int value = 0;
            if (entry.length() != VCF_DIPLOID_WIDTH) { // Simple integrity check.
                value = MISSING_DATA;
            } else {
                if (!(entry.substring(0, 1).equals("0"))) { // Check first allele.
                    ++value;
                }
                if (!(entry.substring(2, 3).equals("0"))) { // Check second allele.
                    ++value;
                }
            }
            result = value;
            break;
        }
        //partialResults[lineNumber] = result;
        //return result;
    }

    /**
     * Returns the number of files that make up results.
     * @return  The number of files in this workers' series.
     */
    public int getNumberOfFilesInSeries() {
        return numberOfFilesInSeries;
    }

}
