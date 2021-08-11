package com.snptools.converter.vcfutilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

/**
 * The VcfToCsvTask class is used to calculate and write the results of the vcf to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.0, June 2021
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
    public VcfToCsvTask(String inputFilename, String outputFilename, int startColumn, int endColumn, int totalColumns, int totalLines) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.totalColumns = totalColumns;
        this.totalLines = totalLines;
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
            String[] lineBuffer = new String[totalLines];
            int sizeOfColumn = 4;
            for (int i = startColumn; i < endColumn; ++i) {
                for (int j = 0; j < totalLines; ++j) {
                    //randomAccessFile.seek(j * i + j * totalColumns * sizeOfColumn);
                    //randomAccessFile.seek(i * sizeOfColumn + j * (totalColumns * sizeOfColumn + 1));
                    //System.out.println(randomAccessFile.length()); // 3476532 total, one line = 1124 -> one column = 4.

                    //randomAccessFile.seek(j * totalColumns * sizeOfColumn);// Correct.
                    randomAccessFile.seek(j * totalColumns * sizeOfColumn + i * sizeOfColumn); // Iterate through column.

                    //System.out.println("totalLines" + totalLines); // 3093.
                    //System.out.println("totalColumns" + totalColumns); // 281.
                    //System.out.println("sizeOfColumn" + sizeOfColumn); // 4.
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


                            case 124: // '|'
                                entry += "|";
                                break;

                            default: // Error or Unknown.
                                entry += "X";
                                break;
                        }

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
                    switch (entry) {
                        case "0/0", "0|0": // 2 Majors.
                            entry = "0";
                            break;
                        case "./.", ".|.", "*/*", "*|*": // Missing data.
                            entry = "4";
                            break;


                        default: // Currently only valid for diploids and assumes data entries are well-formed:
                            int result = 0;
                            if (!(entry.substring(0, 1).equals("0"))) {
                                ++result;
                            }
                            if (!(entry.substring(2, 3).equals("0"))) {
                                //System.out.println(entry.substring(2, 3));
                                ++result;
                            }
                            entry = "" + result;
                            break;
                    }

                    lineBuffer[j] = entry;
                    //System.out.println("Entry:[" + j + "] " + entry);
                }
                outputStreamWriter.write("-9,"); // Phenotype placeholder.
                for (int j = 0; j < totalLines; ++j) {
                    if (j < totalLines - 1) {
                        outputStreamWriter.write(lineBuffer[j] + ",");
                    } else {
                        outputStreamWriter.write("" + lineBuffer[j]);
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

}
