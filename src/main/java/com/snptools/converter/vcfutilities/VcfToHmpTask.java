package com.snptools.converter.vcfutilities;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

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
    //private static final int TRIPLOID_WIDTH = 5;
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
    }

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
            String[] lineBuffer = new String[totalColumns];
            int sizeOfColumn = 4;
            for (int i = startLine; i < endLine; ++i) {
                for (int j = 0; j < totalColumns; ++j) {
                    lineBuffer[j] = "";
                    //randomAccessFile.seek(line + offset);
                    randomAccessFile.seek(i * totalColumns * sizeOfColumn + j * sizeOfColumn);
                    // Iterate through line.

                    //System.out.println("totalLines" + totalLines); // 3093.
                    //System.out.println("totalColumns" + totalColumns); // 281.
                    //System.out.println("sizeOfColumn" + sizeOfColumn); // 4.

                    String entry = "";
                    for (int k = 0; k < DIPLOID_WIDTH; ++k) { // == 3.
                        int value = randomAccessFile.read();

                        switch (value) {
                            case 43: // '+'
                                entry += "+";
                                break;
                            case 44: // ','
                                entry += ",";
                                break;
                            case 45: // '-'
                                entry += "-";
                                break;
                            case 46: // '.'
                                entry += ".";
                                break;
                            case 47: // '/'
                                entry += "/";
                                break;
                            case 48: // '0'
                                entry += "0";
                                break;
                            case 49: // '1'
                                entry += "1";
                                break;
                            case 50: // '2'
                                entry += "2";
                                break;
                            case 51: // '3'
                                entry += "3";
                                break;
                            case 52: // '4'
                                entry += "4";
                                break;
                            case 53: // '5'
                                entry += "5";
                                break;
                            case 54: // '6'
                                entry += "6";
                                break;
                            case 55: // '7'
                                entry += "7";
                                break;
                            case 56: // '8'
                                entry += "8";
                                break;
                            case 57: // '9'
                                entry += "9";
                                break;
                            case 58: // ':'
                                entry += ":";
                                break;
                            case 59: // ';'
                                entry += ";";
                                break;


                            case 124: // '|'
                                entry += "|";
                                break;

                            default: // Error or Unknown.
                                entry += "X";
                                break;
                        }

                    }
                    // Only Diploid cells are currently supported.
                    if (entry.length() == DIPLOID_WIDTH) { // == 3.
                        try {
                            if (entry.compareTo("./.") == 0) {
                                //System.out.println("./. found");
                                lineBuffer[j] = NO_DATA_DIPLOID;
                            } else {
                                // Take the first and last characters, skipping the separator:
                                int positionOne = Integer.parseInt(entry, 0, 1, 10);
                                int positionTwo = Integer.parseInt(entry, 2, 3, 10);

                                //String options = "";

                                String[] values = alleles[i].split(",");
                                String entryOne = "" + values[positionOne];
                                String entryTwo = "" + values[positionTwo];
                                String result = ""; // The output result that will be saved

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
                                lineBuffer[j] = result;
                        /* if alphabetical/strand correction is not desired, use this:
                        //result = "" + options.substring(positionOne, positionOne + 1) + options.substring(positionTwo, positionTwo + 1);
                        //result = "" + alleles[i][positionOne] + alleles[i][positionTwo]
                        */
                            }
                        } catch (NumberFormatException e) {
                            // Thrown when the "no data" entry is encountered: "./."
                            System.out.println("Skipping input. Unexpected data encountered - input data should be an integer.");
                            lineBuffer[j] = NO_DATA_DIPLOID;
                        } catch (IndexOutOfBoundsException e) {
                            System.out.println("Skipping input. Possible malformed VCF file - input data appears inconsistent.");
                            // Likely a malformed file or programming logical error:
                            lineBuffer[j] = NO_DATA_DIPLOID;
                        }
                    }
                }
                // Print file+line header:
                outputStreamWriter.write("" + lineHeaders[i]);
                for (int j = 0; j < totalColumns; ++j) {
                    if (j < totalColumns - 1) {
                        outputStreamWriter.write(lineBuffer[j] + "\t");
                    } else {
                        outputStreamWriter.write("" + lineBuffer[j]);
                    }
                }
                outputStreamWriter.write("\n");
            }
        } catch (IOException e) {
            System.out.println("There was an error accessing files in a VcfToHmpTask worker.");
            e.printStackTrace();
        }

    }

}
