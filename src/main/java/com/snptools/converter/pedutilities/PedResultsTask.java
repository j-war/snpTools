package com.snptools.converter.pedutilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

/**
 * The PedResultsTask class is used to calculate and write the results of the ped to 
 * csv conversion to the provided output file path.
 * <p>
 * The resulting files are to be merged sequentially once the task has completed
 * and the threads were successfully joined.
 * 
 * @author  Jeff Warner
 * @version 1.1, July 2021
 */
public class PedResultsTask implements Runnable {

    private final int COLUMNS_TO_SKIP = 6; // Number of columns in ped header.
    private final int PHENOTYPE_COLUMN = 5; // Position of phenotype in ped header.
    private final int MISSING_DATA = 4; // The SNP at this site is missing from the ped file. Note: the output csv file may have a 4 or 5 for this site. 4 if both alleles were "0" (representing missing data), or 5 if only 1 allele was "0".
    private final String inputFilename; // The input file name with path and extension.
    private final String outputFilename; // The output file name with path and extension.
    private final int startLine;
    private final int endLine;
    private final int columns;  // The number of columns to compare.

    private int[] partialResults; // A line buffer containing accumulated results.
    private String[] phenotypes;
    private final String[] majorAllelesValues; // A reference to calculated major bases.

    /**
     * Compares the input file to the calculated site's major allele and writes the
     * output to the provided file path.
     *
     * @param inputFilename The input file path and file name with a file extension for processing.
     * @param outputFilename    The  The input file path and file name with a file extension for processing.
     * @param startLine The line for this worker to start at.
     * @param endLine   The line for this worker to finish at.
     * @param columns   The number of columns that this worker should compare.
     * @param majorAllelesValues    A reference to the calculated major alleles structure.
     */
    public PedResultsTask(String inputFilename, String outputFilename, int startLine, int endLine, int columns, String[] majorAllelesValues) {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.startLine = startLine;
        this.endLine = endLine;
        this.columns = columns;
        this.partialResults = new int[columns];
        this.phenotypes = new String[endLine - startLine];
        this.majorAllelesValues = majorAllelesValues;
    }

    @Override
    public void run() {
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
            FileOutputStream outputStream = new FileOutputStream(outputFilename);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)
        ) {
            for (int i = 0; i < startLine; ++i) { reader.readLine(); } // Skip ahead to the starting position.
            for (int i = 0; i < endLine - startLine; ++i) {
                accumulateResults(i, reader.readLine());
                // Write the accumulated results:
                outputStreamWriter.write(phenotypes[i]);
                for (int x = 0; x < columns; ++x) {
                    outputStreamWriter.write("," + partialResults[x]);
                }
                outputStreamWriter.write("\n");
            }
        } catch (FileNotFoundException e) {
            System.out.println("The provided file could not be found or it could not be opened.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the input file or writing to the output.");
            e.printStackTrace();
        }
    }

    /**
     * Iterates through the TSV string parsing it into the phenotype and partialResults
     * data structures that will be written to disk after.
     * 
     * @param lineNumber    The line number that is being parsed and indexed into
     *                      phenotypes[] and partialResults[].
     * @param line  The TSV string of the line that was read from the file to be parsed.
     */
    private void accumulateResults(int lineNumber, String line) {
        if (line == null || line.isBlank() || line.isEmpty()) {
            System.out.println("The provided line contained no data.");
            for (int k = 0; k < columns; ++k) {
                partialResults[k] = MISSING_DATA + 1;
            }
            return;
        }
        Scanner lineScanner = new Scanner(line);
        for (int j = 0; j < COLUMNS_TO_SKIP; ++j) { // Skip ahead and collect phenotypes.
            if (lineScanner.hasNext()) {
                if (j == PHENOTYPE_COLUMN) {
                    phenotypes[lineNumber] = lineScanner.next();
                    //phenotypes.add(i, lineScanner.next());
                } else {
                    lineScanner.next();
                }
            } else {
                System.out.println("Malformed ped file: unexpected or missing data.");
                lineScanner.close();
                return;
            }
        }

        // Parse line into results:
        for (int k = 0; k < columns; ++k) {
            int result = 0;
            String value = "";
            if (lineScanner.hasNext()) {
                value = lineScanner.next();
                switch (value) {
                    case "A", "C", "G", "T", "a", "c", "g", "t":
                        if (!value.equalsIgnoreCase(majorAllelesValues[k])) {
                            ++result;
                        }
                        break;
                    default:
                        result = MISSING_DATA; // So if output in file is 4 or 5 then it is unknown or missing data.
                        break;
                }
            } else {
                System.out.println("Malformed ped file: unexpected number of alleles.");
                lineScanner.close();
                return;
            }

            if (lineScanner.hasNext()) {
                value = lineScanner.next();
                switch (value) {
                    case "A", "C", "G", "T", "a", "c", "g", "t":
                        if (!value.equalsIgnoreCase(majorAllelesValues[k])) {
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
            } else {
                System.out.println("Malformed ped file: odd number of alleles.");
                lineScanner.close();
                return;
            }
            partialResults[k] = result;
        }
        lineScanner.close();
    }

}
