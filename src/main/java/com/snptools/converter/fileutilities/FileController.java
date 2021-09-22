package com.snptools.converter.fileutilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
     * @param fileName   The file path with file name and with an extension.
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
     * Use to determine if the directory of the file path with an extension exists.
     * 
     * @param fileName   The file path with file name and with an extension.
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
     * Use to determine if the file path is a directory or not.
     * 
     * @param fileName   The file path with file name and with an extension.
     * @return  Whether the file is a directory or not.
     * 
     * Note: There is no guarantee that the file path will still exist after this method returns.
     */
    public static boolean isADirectory(String fileName) {
        if (fileName == null || fileName.isEmpty() || fileName.isBlank()) {
            System.out.println("Error: The provided file path was empty.");
            return false;
        }
        try {
            File output = new File(fileName);
            return output.isDirectory();
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
     * @param fileName  The path containing the file name with an extension.
     * @return  An integer representing the total number of lines in the text file.
     * 
     * Returns -1 on an exception regardless of if any lines were previously counted.
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
            //e.printStackTrace();
            return -1;
        } catch (IOException e) {
            System.out.println("There was an IO error checking the input file.");
            //e.printStackTrace();
            return -1;
        }
        return totalInputLines;
    }

    /**
     * Merges a set of files, in ordinal number order, and names it as provided.
     * @param count The number of files in the set, from 0 to count - 1, inclusive.
     * @param resultFile    The path containing the name of the resultant file.
     * @param inputName The path containing the file name, with an extension.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     * 
     * Note: Will overwrite files, permissions allowing, without warning.
     */
    public static void mergeFiles(int count, String resultFile, String inputName) throws DiskFullException {
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
            if (pw.checkError()) {
                throw new DiskFullException("Error: An IOException occurred while merging files - the disk may be full.");
            }
        } catch (FileNotFoundException e) {
            System.out.println("An intermediate file appears to be missing.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error attempting to access an intermediate file.");
            e.printStackTrace();
        }
    }

    /**
     * Merges the lines of the series of files into a resultant file.
     * @param lineCount The number of data lines that each file contains.
     * @param fileCount The number of files in the series.
     * @param resultFile    The output location of the merged files with filename and extension.
     * @param tempName  The file path with extension of the files to be merged.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     */
    public static void mergeFilesLines(int lineCount, int fileCount, String resultFile, String tempName) throws DiskFullException {
        try (
            PrintWriter pw = new PrintWriter(resultFile)
        ) {
            // Make a thread and then wait on it
            //synchronized (this) {
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
                if (pw.checkError()) {
                    throw new DiskFullException("Error: An IOException occurred while merging files - the disk may be full.");
                }
            //}
        } catch (FileNotFoundException e) {
            System.out.println("An intermediate file appears to be missing.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was an error attempting to access an intermediate file.");
            e.printStackTrace();
        }
    }

    /**
     * Merges the lines of the series of files into a resultant file.
     * @param lineCount The number of data lines that each file contains.
     * @param fileCount The number of files in the series.
     * @param resultFile    The output location of the merged files with filename and extension.
     * @param portion   The portion of the data that should be merged together with.
     * @throws DiskFullException  If the print writer experiences an error such as a full disk.
     */
    public static void mergeFilePieces(int lineCount, int fileCount, String resultFile, int portion) throws DiskFullException {
        // Merge if there are multiple files, otherwise just rename and move it:
        Path outputFolder = Paths.get(resultFile + portion).getParent().normalize();
        Path outputFile = Paths.get(resultFile + portion).getFileName();
        File tempDir = new File(outputFolder.toString() + "/temp" + portion + "/");
        if (fileCount > 1) {
            mergeFilesLines(
                lineCount,
                fileCount,
                (resultFile + portion),
                "./" + tempDir + "/" + outputFile
            );
        } else {
            try {
                // Rename+move file by removing "...0" and moving it out of the temp directory.
                File originalFile = new File("./" + tempDir + "/" + (resultFile + portion) + "0");
                File newFile = new File(resultFile + portion);
                boolean result = originalFile.renameTo(newFile);
                //System.out.println("Renamed:" + result);
            } catch (NullPointerException e) {
                // Ignore.
                //System.out.println("No files to move.");
            }
        }
    }

    /**
     * Attempts to delete temporary files associated with a requested conversion.
     * @param count The number of files in the set.
     * @param fileName The path containing the file name, with an extension and without an appendix.
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
     * @param fileName  The path containing the file name, with an extension, to be deleted.
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

    /**
     * Attempts to delete a series of temporary directories.
     * @param numberOfFolders  The path containing the file name, with an extension, to be deleted.
     * @param outputFileName  The path containing the file name, with an extension, to be deleted.
     * 
     *  Note: May silently fail to delete temporary files if there are certain security settings on the system.
     *        Will delete and/or overwrite existing data with no warning or prompts.
     */
    public static void deleteTempFolders(int numberOfFolders, String outputFileName) {
        try {
            for (int i = 0; i < numberOfFolders; ++i) {
                Path outputFolder = Paths.get(outputFileName).getParent().normalize();
                File tempDir = new File("./" + outputFolder.toString() + "/temp" + i + "/");
                recursivelyDeleteFolder(tempDir);
            }
        } catch (NullPointerException e) {
            System.out.println("The provided pathname could not be found.");
        } catch (SecurityException e) {
            System.out.println("Unable to delete file. The security manager is denying access to the file.");
        }
    }

    /**
     * Recursively deletes all files and folders while avoiding symbolic links.
     * @param folderToDelete  The folder its content to be deleted.
     * 
     * Note: May not work with certain security settings.
     */
    private static void recursivelyDeleteFolder(File folderToDelete) {
        File[] contents = folderToDelete.listFiles();
        if (contents != null) {
            for (File file : contents) {
                try {
                    if (! Files.isSymbolicLink(file.toPath())) {
                        recursivelyDeleteFolder(file);
                    }
                } catch (InvalidPathException e) {
                    // Ignore this exception:
                    // The file was deleted by another process or it does not exist, simply continue.
                    // e.printStackTrace();
                }
            }
        }
        folderToDelete.delete();
    }
    
    // Merge a list of string arrays and return the result
    static String[] merge(String[] ...arrays) {
        Stream<String> stream = Stream.of();
        for (String[] s : arrays) {
            stream = Stream.concat(stream, Arrays.stream(s));
        }
        return stream.toArray(String[]::new);
    }

    /**
     * Convenience method for filtering and converting read bytes to a specific
     * set of characters.
     * 
     * @param value   The integer that was read through a random access file to be converted.
     * @return  The interpreted character.
     */
    public static char intToChar(int value) {
        char entry = 'X';
        switch (value) {
            case 43:
                entry = '+';
                break;
            case 44:
                entry = ',';
                break;
            case 45:
                entry = '-';
                break;
            case 46:
                entry = '.';
                break;
            case 47:
                entry = '/';
                break;
            case 48:
                entry = '0';
                break;
            case 49:
                entry = '1';
                break;
            case 50:
                entry = '2';
                break;
            case 51:
                entry = '3';
                break;
            case 52:
                entry = '4';
                break;
            case 53:
                entry = '5';
                break;
            case 54:
                entry = '6';
                break;
            case 55:
                entry = '7';
                break;
            case 56:
                entry = '8';
                break;
            case 57:
                entry = '9';
                break;
            case 58:
                entry = ':';
                break;
            case 59:
                entry = ';';
                break;

            // Hmp->Csv
            case 65, 97: // 'A', 'a'
                entry = 'A';
                break;
            case 67, 99: // 'C', 'c'
                entry = 'C';
                break;
            case 71, 103: // 'G', 'g'
                entry = 'G';
                break;
            case 84, 116: // 'T', 't'
                entry = 'T';
                break;

            case 82, 114: // 'R', 'r'
                entry = 'R';
                break;
            case 89, 121: // 'Y', 'y'
                entry = 'Y';
                break;
            case 83, 115: // 'S', 's'
                entry = 'S';
                break;
            case 87, 119: // 'W', 'w'
                entry = 'W';
                break;
            case 75, 107: // 'K', 'k'
                entry = 'K';
                break;
            case 77, 109: // 'M', 'm'
                entry = 'M';
                break;

            case 66, 98: // 'B', 'b'
                entry = 'B';
                break;
            case 68, 100: // 'D', 'd'
                entry = 'D';
                break;
            case 72, 104: // 'H', 'h'
                entry = 'H';
                break;
            case 86, 118: // 'V', 'v'
                entry = 'V';
                break;

            case 78, 110: // 'N', 'n'
                entry = 'N';
                break;

            case 124:
                entry = '|';
                break;

            default: // Error or Unknown.
                entry = 'X';
                break;
        }

        return entry;
    }

}
