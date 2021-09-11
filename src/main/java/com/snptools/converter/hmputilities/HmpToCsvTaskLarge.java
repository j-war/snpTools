package com.snptools.converter.hmputilities;

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
 * The HmpToCsvTaskLarge class is used to calculate and write the results of the hmp to 
 * csv conversion to the provided output file path when the input source is considered a
 * large file (2+gb).
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.4, September 2021
 */
public class HmpToCsvTaskLarge implements Runnable {

    private final int MISSING_DATA = 4; // The SNP at this site is missing from the hmp file. Note: the output csv file may have a 4 or 5 for this site. 4 if both alleles were missing data, or 5 if only 1 allele was.
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.

    private final long startLine;
    private final long endLine;

    private final int portion; // The portion of the total data that this worker is working on.

    private final String[] majorAllelesValues; // A reference to the calculated major alleles.

    volatile private int numberOfFilesInSeries = 0; // Value to be retrieved for merging after this task completes.


    /**
     * Compares the input file to the calculated site's major and writes its output to the provided path.
     * This conversion involves a transpose of HMP columns into CSV lines.
     * The startColumn and endColumn arguments are used for file pointer positioning while the
     * totalColumns and totalLines arguments are used for transposing and transforming the data.
     * 
     * @param inputFilename The input file path and file name and an extension for processing.
     * @param outputFilename    The  The output file path and file name with an extension for processing.
     * @param startLine   The start line for this worker to start at.
     * @param endLine The end line for this worker to finish at.
     * @param majorAllelesValues    A reference to the previously calculated major allleles.
     * @param portion    The worker number assigned to this chunk of the output.
     */
    public HmpToCsvTaskLarge(String inputFilename, String outputFilename, int startLine, int endLine, String[] majorAllelesValues, int portion) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = (long) startLine;
        this.endLine = (long) endLine;
        this.majorAllelesValues = majorAllelesValues;
        this.portion = portion;
    }

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

                int offsetCorrectionFactor = 0; // Used in the tail portion to correct for chunks not divisible by chunkSize.

                for (int j = 0; j < chunkSize; ++j) {
                    inputChunk.add(j, new ArrayList<Integer>());
                    String line = reader.readLine();
                    if (line != null) {
                        if (!(line.isBlank()) || !(line.isEmpty())) {
                            String[] arr = line.split(",");

                            for (String entry : arr) {
                                (inputChunk.get(j)).add(accumulateResults(X + j, entry));
                            }
                        } else { // Line is blank/newline, ignore it by incrementing offset:
                            ++offsetCorrectionFactor;
                        }
                    } else { // total lines not divisible by chunksize - we read a null, increment offset and continue.
                        ++offsetCorrectionFactor;
                    }
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
                for (int j = 0; j < outputChunk.size(); ++j) {
                    String result = "";
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

                //X += (chunkSize - offsetCorrectionFactor);
                X += chunkSize;
            }
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file in a HmpToCsvTaskLarge worker.");
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
    private int accumulateResults(long lineNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            //partialResults[lineNumber] = MISSING_DATA + 1;
            return (MISSING_DATA + 1);
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
                    if (!allele.equalsIgnoreCase(majorAllelesValues[(int) lineNumber])) {
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
        //partialResults[lineNumber] = result;
        return result;
    }

    /**
     * Returns the number of files that make up results.
     * @return  The number of files in this workers' series.
     */
    public int getNumberOfFilesInSeries() {
        return numberOfFilesInSeries;
    }

}
