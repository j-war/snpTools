package com.snptools.converter.vcfutilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

import com.snptools.converter.fileutilities.FileController;

/**
 * The VcfToCsvTask class is used to calculate and write the results of the vcf to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, August 2021
 */
public class VcfToCsvTask implements Runnable {

    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startColumn;
    private final int endColumn;
    private final int totalColumns;
    private final int totalLines;
    //private static final int HAPLOID_WIDTH = 1;
    private static final int DIPLOID_WIDTH = 3; // The number of characters wide including the inner separator.
    //private static final int TRIPLOID_WIDTH = 5;
    private final int NO_DATA_DIPLOID_IN_CSV = 5; // CSV format uses 5 for two missing data points in diploids.
    private static final long SIZE_OF_VCF_COLUMN = 4;
    private int[] partialResults;

    /**
     * This worker completes the conversion from VCF to CSV by utilizing the collected and normalized data.
     * Compares the input file to the calculated site's major and writes its output to the provided path.
     *
     * @param inputFilename The input file path and file name with a file extension for processing.
     * @param outputFilename    The  The input file path and file name with a file extension for processing.
     * @param startColumn   The column for this worker to start at.
     * @param endColumn The column for this worker to finish at.
     * @param totalColumns  The number of columns this worker should process.
     * @param totalLines    The total number of data lines to process.
     */
    @Deprecated
    public VcfToCsvTask(String inputFilename, String outputFilename, int startColumn, int endColumn, int totalColumns, int totalLines) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.totalColumns = totalColumns;
        this.totalLines = totalLines;
        this.partialResults = new int[totalLines];
    }

    @Override
    public void run() {
        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(inputFilename, "r");
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            // For each of the columns,
            // Seek to starting line
            // read a value and store value in arrayBuffer
            // seek to next line
            // read a value store value in arrayBuffer
            // ...
            // at end of file, write arrayBuffer to new line
            // go to start.
            for (int i = startColumn; i < endColumn; ++i) {
                for (int j = 0; j < totalLines; ++j) {
                    final long position = (j * totalColumns * SIZE_OF_VCF_COLUMN) + (i * SIZE_OF_VCF_COLUMN);
                    randomAccessFile.seek(position); // Iterate through column.
                    String entry = "";
                    for (int k = 0; k < DIPLOID_WIDTH; ++k) {
                        entry += FileController.intToChar(randomAccessFile.read());
                    }
                    accumulateResults(j, entry);
                }
                outputStreamWriter.write("-9,"); // Phenotype placeholder.
                for (int j = 0; j < totalLines; ++j) {
                    if (j < totalLines - 1) {
                        outputStreamWriter.write(partialResults[j] + ",");
                    } else {
                        outputStreamWriter.write("" + partialResults[j]);
                    }
                }
                outputStreamWriter.write("\n");
            }
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file in a VcfToCsvTask worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error accessing files in a VcfToCsvTask worker.");
            e.printStackTrace();
        }

    }

    /**
    * Interprets the provided string that was read from the normalized data file
    * and stores the results in the line buffer, partialResults.
    * 
    * @param lineNumber    The line number that is being parsed.
    * @param entry The data entry to be converted from VCF to HMP
    *
    * Note: Does not cover the case of a single missing value in diploid cells
    *       Example: "./#" or "#/." etc.
    *       
    *       If the entry is a bad length then the output will be 5/missing data.
    */
   private void accumulateResults(int lineNumber, String entry) {
       if (entry == null || entry.isBlank() || entry.isEmpty()) {
           System.out.println("The provided entry contained no data.");
           partialResults[lineNumber] = NO_DATA_DIPLOID_IN_CSV;
           return;
       }
       // Check if 2 zeros.
        // Check if error ./.
        // Check if 1 zero
        //   else, two minor alleles.
        /*
        SNP coding: for diploid,
            If we have two major alleles, it is 0.
            One major and one minor, it is 1.
            Two minor alleles, it is 2.
        */
        int result = NO_DATA_DIPLOID_IN_CSV;
        switch (entry) {
            case "0/0", "0|0": // 2 Majors.
                result = 0;
                break;
            case "./.", ".|.", "*/*", "*|*": // Missing data.
                result = NO_DATA_DIPLOID_IN_CSV;
                break;

            default: // Currently only valid for diploids and assumes data entries are well-formed:
                int value = 0;
                if (entry.length() != DIPLOID_WIDTH) { // Simple integrity check.
                    value = NO_DATA_DIPLOID_IN_CSV;
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
        partialResults[lineNumber] = result;
    }

}
