package com.snptools.converter.hmputilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

import com.snptools.converter.fileutilities.FileController;

/**
 * The HmpToCsvTask class is used to calculate and write the results of the hmp to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.2, August 2021
 */
public class HmpToCsvTask implements Runnable {

    private final int SIZE_OF_COLUMN = 3; // Including the comma: Example: "AA," is the column including the comma seperator.
    private final int ploidWidth;   // The width of the data entries representing the ploidiness of the data.
    private final int MISSING_DATA = 4; // The SNP at this site is missing from the hmp file. Note: the output csv file may have a 4 or 5 for this site. 4 if both alleles were missing data, or 5 if only 1 allele was.
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startColumn;
    private final int endColumn;
    private final int totalColumns;
    private final int totalLines;

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
    public HmpToCsvTask(String inputFilename, String outputFilename, int startColumn, int endColumn, int totalColumns, int totalLines, String[] majorAllelesValues, int ploidiness) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.totalColumns = totalColumns;
        this.totalLines = totalLines;
        this.majorAllelesValues = majorAllelesValues;
        this.ploidWidth = ploidiness;
        this.partialResults = new int[totalLines];
    }

    public void run() {
        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(inputFilename, "r");
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            for (int i = startColumn; i < endColumn; ++i) {
                for (int j = 0; j < totalLines; ++j) {
                    final long position = (j * totalColumns * (ploidWidth + 1L)) + i * (ploidWidth + 1L);
                    randomAccessFile.seek(position); // Move pointer into position.
                    /* Convert intermediate value to allele:
                        * The RandomAccessFile.read() function returns bytes:
                        * convert bytes to char, then interpet the string.
                    */
                    String entry = "";
                    for (int k = 0; k < ploidWidth; ++k) {
                        // Column size is 3 but we only want the first two chars while dropping the trailing comma.
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
            System.out.println("There was an error finding a file in a HmpToCsvTask worker.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error accessing files in a HmpToCsvTask worker.");
            e.printStackTrace();
        }

    }

    /**
     * Interprets the HMP entry into the line buffer at the specified line.
     * 
     * @param lineNumber    The line number that is being parsed and indexed into partialResults[].
     * @param entry  Is a two character allele entry from an HMP file
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
