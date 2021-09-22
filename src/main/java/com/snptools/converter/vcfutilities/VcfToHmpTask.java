package com.snptools.converter.vcfutilities;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

import com.snptools.converter.fileutilities.FileController;

/**
 * The VcfToHmpTask class is used to calculate and write the results of the vcf to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 *
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class VcfToHmpTask implements Runnable {

    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int totalColumns;
    private static final String NO_DATA_DIPLOID = "NN"; // HMP representation of missing/unknown data for diploid cells. Two 'N' characters for diploids, three for triploid, etc.
    //private static final int HAPLOID_WIDTH = 1;
    private static final int DIPLOID_WIDTH = 3;
    private static final long SIZE_OF_VCF_COLUMN = 4;
    //private static final int TRIPLOID_WIDTH = 5;
    private String[] partialResults; // A line buffer containing accumulated results.
    private final String[] alleles; // A reference to the collected alleles array.
    private final String[] lineHeaders; // A reference to the line headers array.

    /**
     * This worker completes the conversion from VCF to HMP by utilizing the collected and normalized data.
     * It compares the input file to the calculated site's major and writes its output to the provided path.
     *
     * @param inputFilename The input file path and file name with a file extension for processing.
     * @param outputFilename    The input file path and file name with a file extension for processing.
     * @param startLine The line for this worker to start at.
     * @param endLine   The column for this worker to finish at.
     * @param totalColumns  The number of columns this worker should process.
     * @param alleles   A reference to the alleles array containing entries of csv-strings of the majors and alt/references.
     * @param outputLineHeaders A reference to the prepared line headers for the hmp output file. The first entry also
     *                          contains the files' header in addition to the first entry's line header for ease.
     */
    public VcfToHmpTask(String inputFilename, String outputFilename, int startLine, int endLine, int totalColumns, String[] alleles, String[] outputLineHeaders) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.totalColumns = totalColumns;
        this.alleles = alleles;
        this.lineHeaders = outputLineHeaders;
        this.partialResults = new String[totalColumns];
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
            for (int i = startLine; i < endLine; ++i) {
                for (int j = 0; j < totalColumns; ++j) {
                    //randomAccessFile.seek(line + offset);
                    final long position = (i * totalColumns * SIZE_OF_VCF_COLUMN) + (j * SIZE_OF_VCF_COLUMN);
                    randomAccessFile.seek(position); // Iterate through line.
                    String entry = "";
                    for (int k = 0; k < DIPLOID_WIDTH; ++k) { // == 3.
                        entry += FileController.intToChar(randomAccessFile.read());
                    }
                    accumulateResults(i, j, entry);
                }
                // Print file+line headers:
                outputStreamWriter.write("" + lineHeaders[i]);
                for (int j = 0; j < totalColumns; ++j) {
                    if (j < totalColumns - 1) {
                        outputStreamWriter.write(partialResults[j] + "\t");
                    } else {
                        outputStreamWriter.write("" + partialResults[j]);
                    }
                }
                outputStreamWriter.write("\n");
            }
        } catch (IOException e) {
            System.out.println("There was an error accessing files in a VcfToHmpTask worker.");
            e.printStackTrace();
        }

    }

    /**
     * Note: ONLY supports diploid cells.
     * 
     * Interprets the provided string that was read from the normalized data file
     * and stores the results in the line buffer, partialResults.
     * 
     * @param lineNumber    The line number that is being parsed.
     * @param columnNumber  The column that is being interpreted.
     * @param entry The data entry to be converted from VCF to HMP.
     */
    private void accumulateResults(int lineNumber, int columnNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            partialResults[columnNumber] = NO_DATA_DIPLOID;
            return;
        }
        try {
            partialResults[columnNumber] = "";
            // Only Diploid cells are currently supported.
            if (entry.length() != DIPLOID_WIDTH) { // == 3.
                partialResults[columnNumber] = NO_DATA_DIPLOID;
                return;
            }
            if ((entry.compareTo("./.") == 0) || (entry.compareTo(".|.") == 0)) {
                //System.out.println("./. found");
                partialResults[columnNumber] = NO_DATA_DIPLOID;
            } else {
                // Take the first and last characters, skipping the separator:
                int positionOne = Integer.parseInt(entry, 0, 1, 10);
                int positionTwo = Integer.parseInt(entry, 2, 3, 10);

                //System.out.println("" + positionOne + " " + positionTwo);
                String[] values = alleles[lineNumber].split(",");
                String entryOne = "" + values[positionOne];
                String entryTwo = "" + values[positionTwo];
                String result = ""; // The output result that will be saved
                //System.out.println("" + entryOne + " " + entryTwo);

                /* Adjust to be alphabetical for HMP format:
                    Options: [A, C, G, T, N]
                        A/C, A/G, A/T
                        C/G, C/T
                        G/T
                    Notes:
                    The VCF file may have only a major SNP resulting in the ref/alt being '.'.
                    If the ref/alt is not available in the VCF record, the result in the 
                    HMP file will be an 'N'.
                */

                if (entryOne.compareToIgnoreCase(entryTwo) < 0) {
                    // entryOne is smaller, or equal.
                    result = "" + entryOne + entryTwo;
                } else if (entryOne.compareToIgnoreCase(entryTwo) > 0)  {
                    // entryOne is greater
                    result = "" + entryTwo + entryOne;
                } else {
                    // Equal.
                    result = "" + entryOne + entryTwo;
                }
                partialResults[columnNumber] = result;
            }
        } catch (NumberFormatException e) {
            // Thrown when the "no data" entry is encountered: "./."
            System.out.println("Skipping input. Unexpected data encountered - input data should be an integer.");
            partialResults[columnNumber] = NO_DATA_DIPLOID;
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Skipping input. Possible malformed VCF file - input data appears inconsistent.");
            // Likely a malformed file or programming logical error:
            partialResults[columnNumber] = NO_DATA_DIPLOID;
        }

    }

}
