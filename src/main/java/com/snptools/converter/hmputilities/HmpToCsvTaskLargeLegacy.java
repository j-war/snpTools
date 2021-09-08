
// original HmpToCsvTaskLarge.

package com.snptools.converter.hmputilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * The HmpToCsvTaskLarge class is used to calculate and write the results of the hmp to 
 * csv conversion to the provided output file path when the input source is considered a
 * large file (2+gb).
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.2, August 2021
 */
public class HmpToCsvTaskLargeLegacy implements Runnable {

    private final long ploidWidth;   // The width of the data entries representing the ploidiness of the data.
    private final int MISSING_DATA = 4; // The SNP at this site is missing from the hmp file. Note: the output csv file may have a 4 or 5 for this site. 4 if both alleles were missing data, or 5 if only 1 allele was.
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final long startLine;
    private final long endLine;
    private final long startColumn;
    private final long endColumn;
    private final long totalColumns;
    private final long totalLines;

    //private String[] phenotypes;
    private final String[] majorAllelesValues; // A reference to the calculated major alleles.

    private int[] partialResults; // A line buffer containing accumulated results.

    /**
     * Compares the input file to the calculated site's major and writes its output to the provided path.
     * This conversion involves a transpose of HMP columns into CSV lines.
     * The startColumn and endColumn arguments are used for file pointer positioning while the
     * totalColumns and totalLines arguments are used for transposing and transforming the data.
     * 
     * @param inputFilename The input file path and file name and an extension for processing.
     * @param outputFilename    The  The output file path and file name with an extension for processing.
     * @param startColumn   The column for this worker to start at.
     * @param endColumn The column for this worker to finish at.
     * @param totalColumns  The number of columns this worker should process.
     * @param totalLines    The total number of lines to process.
     * @param majorAllelesValues    A reference to the previously calculated major allleles.
     * @param ploidiness    The width of the data entries representing the ploidiness of the data.
     */
    public HmpToCsvTaskLargeLegacy(String inputFilename, String outputFilename, int startLine, int endLine, int startColumn, int endColumn, int totalColumns, int totalLines, String[] majorAllelesValues, int ploidiness) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = (long) startColumn;
        this.endColumn = (long) endColumn;
        this.startLine = (long) startLine;
        this.endLine = (long) endLine;
        this.totalColumns = (long) totalColumns;
        this.totalLines = (long) totalLines;
        this.majorAllelesValues = majorAllelesValues;
        this.ploidWidth = (long) ploidiness;
        this.partialResults = new int[totalColumns];
    }

    public void run() {
        // Stream input while randomly writing:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            // Read in x lines, transpose+manipulate, write the checked out X.
            List<List<String>> inputChunk = new ArrayList<List<String>>(); // Inner List holds lines/rows.
            List<List<String>> outputChunk = new ArrayList<List<String>>(); // Inner List holds columns.
            
            // Load inputChunk:
            // for (int i = 0; i < startLine; ++i) { reader.readLine(); } // System.out.println("Skipping a line."); } // Skip ahead to starting line.
            for (int i = 0; i < endLine - startLine; ++i) {
                inputChunk.add(i, new ArrayList<String>());
                String[] arr = reader.readLine().split(",");
                for (int j = 0; j < arr.length; ++j) { // for the number of input rows.
                    outputChunk.add(j, new ArrayList<String>());
                }
                for (String entry : arr) {
                    (inputChunk.get(i)).add(entry);
                }
            }
            for (int i = 0; i < inputChunk.get(0).size(); ++i) { // for the number of input rows.
                outputChunk.add(i, new ArrayList<String>());
            }

            // Create a big buffer to hold read lines that will be transposed:
            // Transpose inputChunk to outputChunk:
            for (int i = 0; i < inputChunk.size(); ++i) { // for the number of input rows.
                for (int j = 0; j < (inputChunk.get(i)).size(); ++j) { // for the number of entries on a line.
                    (outputChunk.get(j)) .add(i, (inputChunk.get(i)).get(j));
                }
            }
            // Pass outputChunk through accumulateResults():
            for (int j = 0; j < totalLines; ++j) { //totalColumns endColumn - startColumn
                // Pass one output line to accumulate, then write it:
                for (int k = 0; k < totalColumns; ++k) {
                    accumulateResults(j, (outputChunk.get(j)).get(k));
                }

                // Write line:
                for (int i = 0; i < outputChunk.size(); ++i) {
                    String result = "";
                    for (int x = 0; x < outputChunk.get(i).size(); ++x) {
                        if (x < outputChunk.get(i).size() - 1) {
                            result += ((outputChunk.get(i)).get(x) + ",");
                        } else {
                            result += ((outputChunk.get(i)).get(x) + "\n");
                        }
                    }
                    outputStreamWriter.write(result);
                    //outputStreamWriter.write(outputChunk.get(i).toString() + "\n");
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file in a HmpSumTask worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the intermediate file.");
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Index does not match expectation. Possible malformed HMP file.");
            e.printStackTrace();
        }
    }

    /**
     * Interprets the HMP entry into the line buffer at the specified line.
     * 
     * @param lineNumber    The line number that is being parsed and indexed into partialResults[].
     * @param entry  Is a character allele entry from an HMP file. Width is equal to ploidiness.
     */
    private void accumulateResults(int lineNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            partialResults[lineNumber] = MISSING_DATA + 1;
            return;
        }
        int result = 0;
        // Compare allele to majorAllelesValues array to determine output:
        for (int k = 0; k < entry.length(); ++k) {
            String allele = entry.substring(k, 1 + k);
            switch (allele) {
                case "A", "C", "G", "T",
                     "a", "c", "g", "t",
                     "R", "Y", "S", "W", "K", "M",
                     "r", "y", "s", "w", "k", "m",
                     "B", "D", "H", "V", "N",
                     "b", "d", "h", "v", "n",
                     ".", "-":
                    if (!allele.equalsIgnoreCase(majorAllelesValues[lineNumber])) {
                        ++result;
                    }
                    break;
                default:
                    if (result == MISSING_DATA) { // if the first allele was missing, set it to 5 by adding 1.
                        ++result;
                    } else { // else, if just the second allele is missing(regardless of it being major/minor) set it to 4.
                        result = MISSING_DATA; // So if output in file is 4 or 5 then it is unknown or missing data.
                    }
                    break;
            }
        }
        // Save result to linebuffer:
        partialResults[lineNumber] = result;
    }

}
