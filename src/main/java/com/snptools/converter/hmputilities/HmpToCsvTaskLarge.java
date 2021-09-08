package com.snptools.converter.hmputilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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
    private final long totalLines;

    private final long startColumn;
    private final long endColumn;
    private final long totalColumns;

    private final int portion; // The portion of the total data that this worker is working on.

    private final String[] majorAllelesValues; // A reference to the calculated major alleles.


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
    public HmpToCsvTaskLarge(String inputFilename, String outputFilename, int startLine, int endLine, int startColumn, int endColumn, int totalColumns, int totalLines, String[] majorAllelesValues, int portion) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startColumn = (long) startColumn;
        this.endColumn = (long) endColumn;
        this.startLine = (long) startLine;
        this.endLine = (long) endLine;
        this.totalColumns = (long) totalColumns;
        this.totalLines = (long) totalLines;
        this.majorAllelesValues = majorAllelesValues;
        this.portion = portion;
    }

    public void run() {
        // Stream input while randomly writing:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            int numberOfFilesInSeries = 0;
            long X = startLine;
            long Y = endLine;
            int chunkSize = 1000; // Tuning parameter.

            // Create temp folder - assume we will need folders:
            Path outputFolder = Paths.get(outputFilename).getParent().normalize();
            Path outputFile = Paths.get(outputFilename).getFileName();
            File tempDir = new File(outputFolder.toString() + "/temp" + portion + "/");
            if (!tempDir.exists()){
                tempDir.mkdir();
            } // else { System.out.println("Temp directory already exists."); }

            //System.out.println("outputFile:[" + outputFile + "]"); // out.txt
            //System.out.println("outputFilename:[" + outputFilename + "]"); // ./Output/out.txt
            //System.out.println("parent folder:[" + outputFolder + "]"); // Output
            //System.out.println("temp folder:[" + tempDir + "]"); // Output/tempX
            // "./" + tempDir + "/" + outputFile + i
            // ./Output/tempX/out.txt1

            while (X < Y) {
                List<List<Integer>> inputChunk = new ArrayList<List<Integer>>(); // Inner List holds lines/rows.
                List<List<Integer>> outputChunk = new ArrayList<List<Integer>>(); // Inner List holds columns.

                int offsetCorrectionFactor = 0;

                for (int j = 0; j < chunkSize; ++j) {
                    inputChunk.add(j, new ArrayList<Integer>());
                    String line = reader.readLine();
                    if (line != null) {
                        if (!(line.isBlank()) || !(line.isEmpty())) {
                            String[] arr = line.split(",");

                            // Initialize the output's inner list:
                            for (int k = 0; k < arr.length; ++k) { // For the number of input columns:
                                //outputChunk.add(k, new ArrayList<Integer>());
                            }

                            for (String entry : arr) {
                                //System.out.println("Entry:[" + entry + "]");
                                //(inputChunk.get(i)).add(entry);
                                (inputChunk.get(j)).add(accumulateResults(X + j, entry));
                            }
                        } else {
                            //System.out.println("Line is blank/newline only");
                            ++offsetCorrectionFactor;
                        }
                    } else {
                        //System.out.println("total lines not divisible by chunksize");
                        ++offsetCorrectionFactor;
                    }
                } // Done reading in a chunk of input.



                // Transpose the processed lines to output lists:

                // The length/size of the first line is the number of lines in the output:
                for (int j = 0; j < inputChunk.get(0).size(); ++j) { // For the number of input columns, add an output row:
                    outputChunk.add(j, new ArrayList<Integer>());
                }

                // Transpose inputChunk to outputChunk:
                for (int j = 0; j < inputChunk.size(); ++j) { // for the number of input rows.
                    for (int k = 0; k < (inputChunk.get(j)).size(); ++k) { // for the number of entries on a line (input columns)
                        (outputChunk.get(k)) .add(j, (inputChunk.get(j)).get(k));
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

                X += (chunkSize - offsetCorrectionFactor);
                //System.out.println("X: " + X);
                //System.out.println("offsetCorrectionFactor: " + offsetCorrectionFactor);
            }

            // Merge if there are multiple files, otherwise just rename:
            if (numberOfFilesInSeries > 1) {
                mergeFiles((int) (endLine - startLine), numberOfFilesInSeries, outputFilename, "./" + tempDir + "/" + outputFile);
            } else {
                // Rename+move file by removing "..0" and moving it out of the temp directory.
                File originalFile = new File("./" + tempDir + "/" + outputFile + "0");
                File newFile = new File(outputFilename);
                boolean result = originalFile.renameTo(newFile);
                //System.out.println("Renamed:" + result);
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

    private void mergeFiles(int lineCount, int fileCount, String resultFile, String tempName) {
        //FileController.mergeFiles(count, resultFile, tempName);
        try (
            PrintWriter pw = new PrintWriter(resultFile)
        ) {
            // Make a list of temporary files and open them all:
            BufferedReader[] filesToRead = new BufferedReader[fileCount];
            for (int i = 0; i < fileCount; ++i) {
                filesToRead[i] = new BufferedReader(new FileReader(tempName + i));
            }

            // Read in 1 line from each:
            for (int i = 0; i < lineCount; ++i) {
                String line = "";
                for (int j = 0; j < fileCount; ++j) {
                    if (j == 0) {
                        line += ((filesToRead[j]).readLine());
                    } else {
                        line += ("," + (filesToRead[j]).readLine());
                    }
                }
                pw.println(line);
            }
            pw.flush();

            // Close the files:
            for (int i = 0; i < fileCount; ++i) {
                (filesToRead[i]).close();
            }
        } catch (FileNotFoundException e) {
            System.out.println("An intermediate file appears to be missing.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error attempting to access an intermediate file.");
            e.printStackTrace();
        }
    }

}
