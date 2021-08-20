package com.snptools.converter.hmputilities;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

import com.snptools.converter.fileutilities.FileController;

/**
 * The HmpToVcfTask class is used to calculate and write the results of the hmp to 
 * vcf conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.2, August 2021
 */
public class HmpToVcfTask implements Runnable {

    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int totalColumns;
    //private static final int HAPLOID_WIDTH = 1;
    private final int ploidWidth;
    //private static final int TRIPLOID_WIDTH = 3;
    private final int SIZE_OF_HMP_COLUMN = 3;
    private final String[] alleles; // A reference to the collected alleles array.
    private final String[] strandDirections; // A reference to the collected strands directions array.
    private final String[] lineHeaders; // A reference to the line headers array.
    private String[] partialResults; // A line buffer containing accumulated results.

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
    public HmpToVcfTask(String inputFilename, String outputFilename, int startLine, int endLine, int totalColumns, String[] alleles, String[] strandDirections, String[] outputLineHeaders, int ploidiness) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.totalColumns = totalColumns;
        this.alleles = alleles;
        this.strandDirections = strandDirections;
        this.lineHeaders = outputLineHeaders;
        this.ploidWidth = ploidiness;
        this.partialResults = new String[totalColumns];
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
            for (int i = startLine; i < endLine; ++i) {
                for (int j = 0; j < totalColumns; ++j) {
                    // randomAccessFile.seek(line + offset);
                    randomAccessFile.seek(i * totalColumns * (ploidWidth + 1) + j * (ploidWidth + 1)); // Move to starting position.
                    // Read an entry from the normalized file and then compare and optionally correct
                    // for the strand direction.
                    // The comparison is between the collected CSV string and the individual alleles
                    // that make up the "entry" variable:
                    // Note: HMP files store character data while VCF stores an index that is relative to
                    //       its own record and possible SNPs.
                    String entry = "";
                    for (int k = 0; k < ploidWidth; ++k) {
                        // Column size is X+1 but we only want the first X chars while dropping the trailing comma.
                        entry += FileController.intToChar(randomAccessFile.read());
                    }
                    accumulateResults(i, j, entry);
                }
                // Print file+line header:
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
            System.out.println("There was an error accessing files in a HmpToVcfTask worker.");
            e.printStackTrace();
        }
    }

    /**
     * Interprets the provided HMP entry and stores its result in the partialResults
     * datastructure.
     * 
     * @param lineNumber    The line number that is being parsed.
     * @param columnNumber    The column number that is being parsed.
     * @param entry  The two character HMP string that was read from the file to be parsed.
     */
    private void accumulateResults(int lineNumber, int columnNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            partialResults[columnNumber] = "./.";
            return;
        }

        // get entry + ploidiness
        // parse ploidWidth-number of entries from entry into String[] of size = ploidWidth
        //     order into the array is based on strand value
        //
        // Split alleles into String[] values
        // Iterate through values array
        // Convert parsed entries into integers based on index of values array
        // 
        // do a final check that it's not "." and construct the result.

        try {
            String parsedEntries[] = new String[ploidWidth];
            // Check the strand direction:
            if (strandDirections[lineNumber].equalsIgnoreCase("+")) { // if strand == +, then iterate from end to start
                for (int i = entry.length(); i > 0; --i) {
                    parsedEntries[Math.abs(i - entry.length())] = "" + entry.substring(i - 1, i);
                }
            } else { // else strand == -, and then iterate from start to end.
                for (int i = 0; i < entry.length(); ++i) {
                    parsedEntries[i] = "" + entry.substring(0 + i, 1 + i);
                }
            }


            String[] values = alleles[lineNumber].split(",");

            String parsedEntriesIndices[] = new String[ploidWidth];
            // Compare entries to values[] to determine index, append index to result.
            for (int k = 0; k < parsedEntriesIndices.length; ++k) {
                //String entryToTest = parsedEntriesResults[k];
                for (int j = 0; j < values.length; ++j) { // Iterate through values
                    if ((parsedEntries[k]).compareToIgnoreCase(values[j]) == 0) {
                        parsedEntriesIndices[k] = "" + j;
                    }
                }
            }
            String results = "";
            for (int k = 0; k < parsedEntriesIndices.length; ++k) {
                if (((parsedEntries[k]).compareToIgnoreCase(".") == 0) || ((parsedEntries[k]).compareToIgnoreCase("-") == 0)) {
                    results += ".";
                } else {
                    if (k < parsedEntriesIndices.length - 1) {
                        results += ("" + parsedEntriesIndices[k] + "/");
                    } else {
                        results += ("" + parsedEntriesIndices[k]);
                    }
                }
            }
            partialResults[columnNumber] = results;

        } catch (IndexOutOfBoundsException e) {
            System.out.println("Skipping input. Possible malformed HMP file - index is out of range of collected data.");
            // Likely a malformed file or programming logical error:
            String errorResult = "";



            
            // Temporary coverage:
            switch (ploidWidth) {
                case 1:
                    errorResult = ".";
                    break;
                case 2:
                    errorResult = "./.";
                    break;
                case 3:
                    errorResult = "././.";
                    break;
                case 4:
                    errorResult = "./././.";
                    break;
                default:
                    errorResult = ".";
                    break;
            }
            partialResults[columnNumber] = errorResult;
        }



/*
        if (ploidWidth == 10) {
            //System.out.println("entry: [" + entry + "]");
            try {
                String result = ".";

                String[] values = alleles[lineNumber].split(",");

                for (int k = 0; k < values.length; ++k) { // Convert entry to index into the collected values:
                    if (entry.compareToIgnoreCase(values[k]) == 0) {
                        result = "" + k;
                    }
                }
                // If the entry was not found in the previously collected alleles array, assign a blank value:
                if (entry.compareToIgnoreCase(".") == 0) {
                    result = ".";
                }
                partialResults[columnNumber] = result;
            } catch (IndexOutOfBoundsException e) {
                System.out.println("Skipping input. Possible malformed HMP file - index is out of range of collected data.");
                // Likely a malformed file or programming logical error:
                partialResults[columnNumber] = ".";
            }

        } else if (ploidWidth == 20) {
            try {
                String entryOne = "";
                String entryTwo = "";
                // Check the strand direction:
                if (strandDirections[lineNumber].equalsIgnoreCase("+")) { // if strand == +
                    entryOne = "" + entry.substring(1, 2);
                    entryTwo = "" + entry.substring(0, 1);
                } else { // else strand == -
                    entryOne = "" + entry.substring(0, 1);
                    entryTwo = "" + entry.substring(1, 2);
                }
                String entryOneResult = ".";
                String entryTwoResult = ".";
                String result = "";

                String[] values = alleles[lineNumber].split(",");

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
                partialResults[columnNumber] = result;
            } catch (IndexOutOfBoundsException e) {
                System.out.println("Skipping input. Possible malformed HMP file - index is out of range of collected data.");
                // Likely a malformed file or programming logical error:
                partialResults[columnNumber] = "./."; // Two '.' characters for diploids, three for triploid, etc.
            }
        }*/


    }

}
