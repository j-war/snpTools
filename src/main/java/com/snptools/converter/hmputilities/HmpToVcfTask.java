package com.snptools.converter.hmputilities;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

/**
 * The HmpToVcfTask class is used to calculate and write the results of the hmp to 
 * vcf conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class HmpToVcfTask implements Runnable {

    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int totalColumns;
    //private static final int HAPLOID_WIDTH = 1;
    private static final int DIPLOID_WIDTH = 2;
    //private static final int TRIPLOID_WIDTH = 3;
    private final String[] alleles; // A reference to the collected alleles array.
    private final String[] strandDirections; // A reference to the collected strands directions array.
    private final String[] lineHeaders; // A reference to the line headers array.

    /**
     * This worker completes the conversion from HMP to VCF by utilizing the collected and normalized data.
     * Compares the input file to the calculated site's major and writes its output to the provided path.
     * 
     * @param inputFilename The input file path and file name with a file extension for processing.
     * @param outputFilename    The  The input file path and file name with a file extension for processing.
     * @param startLine   The line for this worker to start at.
     * @param endLine The column for this worker to finish at.
     * @param totalColumns  The number of columns this worker should process.
     * @param alleles   A reference to the alleles array containing entries of csv-strings of the majors and alt/references.
     * @param outputLineHeaders   A reference to the prepared line headers for the hmp output file. The first entry also
     *                              contains the files' header in addition to the first entry's line header for ease.
     */
    public HmpToVcfTask(String inputFilename, String outputFilename, int startLine, int endLine, int totalColumns, String[] alleles, String[] strandDirections, String[] outputLineHeaders) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.totalColumns = totalColumns;
        this.alleles = alleles;
        this.strandDirections = strandDirections;
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
            int sizeOfColumn = 3;
            for (int i = startLine; i < endLine; ++i) {
                for (int j = 0; j < totalColumns; ++j) {
                    // randomAccessFile.seek(line + offset);
                    randomAccessFile.seek(i * totalColumns * sizeOfColumn + j * sizeOfColumn); // Move to starting position.

                    // Read an entry from the normalized file and then compare and optionally correct
                    // for the strand direction.
                    // The comparison is between the collected CSV string and the individual alleles
                    // that make up the "entry" variable:
                    // Note: HMP files store character data while VCF stores an index that is relative to
                    //       its own record and possible SNPs.
                    String entry = "";
                    for (int k = 0; k < DIPLOID_WIDTH; ++k) {
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


                            case 65, 97: // 'A', 'a'
                                entry += "A";
                                break;
                            case 67, 99: // 'C', 'c'
                                entry += "C";
                                break;
                            case 71, 103: // 'G', 'g'
                                entry += "G";
                                break;
                            case 84, 116: // 'T', 't'
                                entry += "T";
                                break;
                            case 78, 110: // 'N', 'n'
                                entry += "N";
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
                    if (entry.length() == DIPLOID_WIDTH) { // == 2.
                        try {
                            String entryOne = "";
                            String entryTwo = "";
                            // Check the strand direction:
                            if (strandDirections[i].equalsIgnoreCase("+")) { // if strand == +
                                entryOne = "" + entry.substring(1, 2);
                                entryTwo = "" + entry.substring(0, 1);
                            } else { // else strand == -
                                entryOne = "" + entry.substring(0, 1);
                                entryTwo = "" + entry.substring(1, 2);
                            }
                            String entryOneResult = ".";
                            String entryTwoResult = ".";
                            String result = "";

                            String[] values = alleles[i].split(",");

                            // Compare entryOne and entryTwo to values[] to determine index, append index to result.
                            // Append result to the lineBuffer.

                            for (int k = 0; k < values.length; ++k) {
                                //System.out.println("entry: [" + entry + "]");
                                if (entryOne.compareToIgnoreCase(values[k]) == 0) {
                                    entryOneResult = "" + k;
                                }
                                if (entryTwo.compareToIgnoreCase(values[k]) == 0) {
                                    entryTwoResult = "" + k;
                                }
                            }
                            if (entryOne.compareToIgnoreCase(".") == 0 || entryTwo.compareToIgnoreCase(".") == 0) {
                                result = "./.";
                            } else {
                                result = entryOneResult + "/" + entryTwoResult;
                            }

                            lineBuffer[j] = result;

                        } catch (IndexOutOfBoundsException e) {
                            System.out.println("Skipping input. Possible malformed HMP file - index is out of range of collected data.");
                            // Likely a malformed file or programming logical error:
                            lineBuffer[j] = "./."; // Two '.' characters for diploids, three for triploid, etc.
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
            System.out.println("There was an error accessing files in a HmpToVcfTask worker.");
            e.printStackTrace();
        }

    }

}
