package com.snptools.converter.hmputilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

/**
 * The HmpToCsvTask class is used to calculate and write the results of the hmp to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class HmpToCsvTask implements Runnable {

    private final int SIZE_OF_COLUMN = 3; // Including the comma: Example: "AA," is the column including the comma seperator.
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startColumn;
    private final int endColumn;
    private final int totalColumns;
    private final int totalLines;

    //private String[] phenotypes;
    private final String[] majorAllelesValues; // A reference to the calculated major alleles.

    /**
     * Compares the input file to the calculated site's major and writes its output to the provided path.
     *
     * @param inputFilename The input file path and file name and an extension for processing.
     * @param outputFilename    The  The output file path and file name with an extension for processing.
     * @param startColumn   The column for this worker to start at.
     * @param endColumn The column for this worker to finish at.
     * @param totalColumns  The number of columns this worker should process.
     * @param totalLines    The total number of lines to process.
     * @param majorAllelesValues    A reference to the previously calculated major allleles.
     */
    public HmpToCsvTask(String inputFilename, String outputFilename, int startColumn, int endColumn, int totalColumns, int totalLines, String[] majorAllelesValues) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.totalColumns = totalColumns;
        this.totalLines = totalLines;
        this.majorAllelesValues = majorAllelesValues;
    }

    public void run() {
        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(inputFilename, "r");
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            String[] lineBuffer = new String[totalLines];
            for (int i = startColumn; i < endColumn; ++i) {
                for (int j = 0; j < totalLines; ++j) {
                    randomAccessFile.seek(j * totalColumns * SIZE_OF_COLUMN + i * SIZE_OF_COLUMN); // Move pointer into position.
                    String entry = "";
                    int result = 0;
                    for (int k = 0; k < SIZE_OF_COLUMN - 1; ++k) {
                        int value = randomAccessFile.read();
                        /* Convert intermediate value to allele:
                         * The RandomAccessFile.read() function returns bytes:
                         * convert bytes to char, then interpet the string.
                        */
                        switch (value) {
                            case 43: // '+'
                                entry = "+";
                                break;
                            case 44: // ','
                                entry = ",";
                                break;
                            case 45: // '-'
                                entry = "-";
                                break;
                            case 46: // '.'
                                entry = ".";
                                break;
                            case 47: // '/'
                                entry = "/";
                                break;
                            case 48: // '0'
                                entry = "0";
                                break;
                            case 49: // '1'
                                entry = "1";
                                break;
                            case 50: // '2'
                                entry = "2";
                                break;
                            case 51: // '3'
                                entry = "3";
                                break;
                            case 52: // '4'
                                entry = "4";
                                break;
                            case 53: // '5'
                                entry = "5";
                                break;
                            case 54: // '6'
                                entry = "6";
                                break;
                            case 55: // '7'
                                entry = "7";
                                break;
                            case 56: // '8'
                                entry = "8";
                                break;
                            case 57: // '9'
                                entry = "9";
                                break;
                            case 58: // ':'
                                entry = ":";
                                break;
                            case 59: // ';'
                                entry = ";";
                                break;


                            case 65, 97: // 'A', 'a'
                                entry = "A";
                                break;
                            case 67, 99: // 'C', 'c'
                                entry = "C";
                                break;
                            case 71, 103: // 'G', 'g'
                                entry = "G";
                                break;
                            case 84, 116: // 'T', 't'
                                entry = "T";
                                break;
                            case 78, 110: // 'N', 'n'
                                entry = "N";
                                break;


                            case 124: // '|'
                                entry = "|";
                                break;

                            default: // Error or Unknown.
                                entry = "X";
                                break;
                        }

                        // Compare allele to majorAllelesValues array to determine output:
                        switch (entry) {
                            case "A", "C", "T", "G", "a", "c", "t", "g":
                                if (!entry.equalsIgnoreCase(majorAllelesValues[j])) {
                                    ++result;
                                }
                                break;
                            default:
                                result = 4; // So, if output in file is 4 or 5 then it is unknown.
                                break;
                        }

                        // Save result to linebuffer:
                        lineBuffer[j] = "" + result;
                    }
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
            System.out.println("There was an error finding a file in a HmpToCsvTask worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error accessing files in a HmpToCsvTask worker.");
            e.printStackTrace();
        }

    }

}
