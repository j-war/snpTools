package com.snptools.converter.fileutilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * The FileHandler class provides access to common static methods related to temporary
 * staging files as well as a method for determining line count.
 * @author  Jeff Warner
 * @version 1.1, August 2021
 */
public class FileController {

    /**
     * Use to determine if a file exists and is readable at this point in time.
     * 
     * @param fileName   The file path with file name and without an extension.
     * @param fileExtension The file's extension.
     * @return  Whether the file exists and is readable.
     * 
     * Note: There is no guarantee that the file will still exist or be readable 
     *       after this method returns.
     */
    public static boolean canReadFile(String fileName) {
        if (fileName == null || fileName.isEmpty() || fileName.isBlank()) {
            System.out.println("Error: The provided file name was empty.");
            return false;
        }
        try {
            File file = new File(fileName);
            if (file.exists()) {
                return file.canRead();
            } else { // Else, the file does not exist.
                return false;
            }
        } catch (NullPointerException e) {
            System.out.println("Error: The provided file path name could not be found.");
            return false;
        } catch (SecurityException e) {
            System.out.println("Error: Unable to check the file. The security manager is denying access to the file.");
            return false;
        }
    }

    /**
     * Use to determine if the directory of the file path without an extension exists.
     * 
     * @param fileName   The file path with file name and without an extension.
     * @return  Whether the file's directory exists.
     * 
     * Note: There is no guarantee that the file path will still exist after this method returns.
     */
    public static boolean directoryExists(String fileName) {
        if (fileName == null || fileName.isEmpty() || fileName.isBlank()) {
            System.out.println("Error: The provided file path was empty.");
            return false;
        }
        try {
            String parentPath = (new File(fileName)).getParent();
            File folder = new File(parentPath);
            if (folder != null && folder.exists()) {
                return folder.isDirectory();
            } else {
                return false; // Folder DNE therefore it is unreadable.
            }
        } catch (NullPointerException e) {
            System.out.println("Error: The provided file path name could not be found or it was empty.");
            return false;
        } catch (SecurityException e) {
            System.out.println("Unable to check the file. The security manager is denying access to the file.");
            return false;
        }
    }

    /**
     * Counts and returns the total number of lines in the provided text file.
     * @param fileName  The path containing the file name without an extension.
     * @return  An integer representing the total number of lines in the text file.
     * 
     * Returns 0 on an exception regardless of if any lines were previously counted.
     */
    public static int countTotalLines(String fileName) {
        int totalInputLines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            int lines = 0;
            while (reader.readLine() != null) {
                ++lines;
            }
            totalInputLines = lines; // Save the total.
        } catch (FileNotFoundException e) {
            System.out.println("The input file could not be found.");
            e.printStackTrace();
            return 0;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            e.printStackTrace();
            return 0;
        }
        return totalInputLines;
    }

    /**
     * Merges a set of files, in ordinal number order, and names it as provided.
     * @param count The number of files in the set, from 0 to count - 1, inclusive.
     * @param resultFile    The path containing the name of the resultant file.
     * @param inputName The path containing the file name, without an extension.
     * 
     * Note: Will overwrite files, permissions allowing, without warning.
     */
    public static void mergeFiles(int count, String resultFile, String inputName) {
        try (
            PrintWriter pw = new PrintWriter(resultFile)
        ) {
            BufferedReader br = null;
            for (int i = 0; i < count; ++i) {
                br = new BufferedReader(new FileReader(inputName + i));
                // Loop to copy content of first file into secondary file:
                String line = "";
                while (null != (line = br.readLine())) {
                    pw.println(line);
                }
                br.close();
            }
            pw.flush();
        } catch (FileNotFoundException e) {
            System.out.println("An intermediate file appears to be missing.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error attempting to access an intermediate file.");
            e.printStackTrace();
        }
    }

    /**
     * Attempts to delete temporary files associated with a requested conversion.
     * @param count The number of files in the set.
     * @param fileName The path containing the file name, without an extension and without an appendix.
     * @param appendix  The appendix added to the file's name based on processing stage.
     * 
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    public static void cleanUp(int count, String fileName, String appendix) {
        if (count > 0) {
            for (int i = 0; i < count; ++i) {
                try {
                    if ( !(new File(fileName + appendix + i).delete()) ) {
                        System.out.println("Could not delete: " + fileName + appendix + i);
                    }
                } catch (NullPointerException e) {
                    System.out.println("The provided pathname could not be found.");
                } catch (SecurityException e) {
                    System.out.println("Unable to delete file. The security manager is denying access to the file.");
                }
            }
        }
    }

    /**
     * Attempts to delete a single file at the provided path.
     * @param fileName  The path containing the file name, without an extension, to be deleted.
     * @param extension The input file's extension.
     * 
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    public static void deleteSingleFile(String fileName) {
        try {
            if ( !(new File(fileName).delete() )) {
                System.out.println("Could not delete: " + fileName);
            }
        } catch (NullPointerException e) {
            System.out.println("The provided pathname could not be found.");
        } catch (SecurityException e) {
            System.out.println("Unable to delete file. The security manager is denying access to the file.");
        }
    }
    
    // Merge a list of string arrays and return the result
    static String[] merge(String[] ...arrays) {
        Stream<String> stream = Stream.of();
        for (String[] s : arrays) {
            stream = Stream.concat(stream, Arrays.stream(s));
        }
        return stream.toArray(String[]::new);
    }
    // Writes the results and phenotype datastructures to the previously
    // named output file. May throw FileNotFoundException if the file
    // cannot be created for some reason.
    /*
    void writeOutput() throws FileNotFoundException {
        FileOutputStream outputStream = new FileOutputStream(csvFileName);
        try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream)) {
            for (int i = 0; i < inputLineCount; ++i) { // Lines.
                outputStreamWriter.write(phenotypes[i]);
                for (int j = 0; j < inputColumnCount; ++j) { // Columns.
                    outputStreamWriter.write("," + results.get(i)[j]);
                }
                outputStreamWriter.write("\n");
            }
            System.out.println("Successfully wrote file.");
        } catch (Exception e) {
            System.out.println("An error occurred writing the file.");
            e.printStackTrace();
        }
    }*/

}
