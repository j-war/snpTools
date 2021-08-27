package com.snptools.converter.hmputilities;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/* PoC test class for arbitrary size matrix transpose, not in memory.
 *
*/
class TEST_PoC_MainChannel_Transposer {
    public final static String[] majorAllelesValues = new String[]{"A", "C", "T", "G", "N"}; // size = number of lines.
    public static int[] partialResults;
    public final static long ploidWidth = 1;
    public final static int MISSING_DATA = 4;
    public final static long startLine = 0;
    public final static long endLine = 4;
    public final static long startColumn = 0;
    public final static long endColumn = 5;

    public static void mainTest(String[] args) {
        final String inputFilename = "./Input/in.txt";//args[0];
        final String outputFilename = "./Output/out.txt";
        final long totalColumns = endColumn - startColumn;
        final long totalLines = endLine - startLine; // the number of lines with data.

        partialResults = new int[(int) (endLine - startLine)];

        start(inputFilename, outputFilename, totalLines, totalColumns);
    }

    public static void start(String inputFilename, String outputFilename, long totalLines, long totalColumns) {
        // Stream input while randomly writing:
        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
            RandomAccessFile writer = new RandomAccessFile(outputFilename, "rw");
            FileChannel channel = writer.getChannel()
        ) {
            //for (int i = 0; i < startLine; ++i) { reader.readLine(); System.out.println("Skipping a line."); } // Skip ahead to starting line.
            for (int i = (int) startLine; i < endLine; ++i) {//endLine
                // 4.
                String line = reader.readLine();
                System.out.println("Line: [" + line + "]");
                //accumulateResults(i, line);

                if (line != null) {
                    Scanner lineScanner = new Scanner(line);
                    lineScanner.useDelimiter(",");

                    //System.out.println("totalColumns: [" + totalColumns + "]");

                    for (int j = 0; j < totalColumns; ++j) {
                        //System.out.println("line length:[" + line.length() + "]");
                        //System.out.println("Line: " + line);
                        if (lineScanner.hasNext()) {
                            String value = lineScanner.next();


                            if (value.length() >= 1) {
                                accumulateResults(i, value);
                                System.out.println("Value: [" + value + "] | partialResults[" + i + "]=[" + partialResults[i] + "]");
                                final long position = (j * totalLines * 2) + (i * 1 * 2);

                                /*
                                ByteBuffer buff = ByteBuffer.wrap((partialResults[i] + "\n").getBytes(StandardCharsets.UTF_8));

                                i = source line
                                j = source columns
                                source column determines dest line
                                source line determines dest column

                                (dest position) =  (dest line) + (dest offset)
                                    (dest line) = j * (lineWidth of dest) * 2
                                    (dest offset) = i * (width of one dest entry) * 2
                                */
                                if (i < totalLines - 1) { // if not the last LINE of the input
                                    ByteBuffer b1 = ByteBuffer.wrap(   (value + ",").getBytes()  ); //partialResults[i]
                                    while (b1.remaining() > 0) {
                                        channel.write(b1, position);
                                    }
                                } else { // if not the last column
                                    ByteBuffer b1 = ByteBuffer.wrap(   (value + '\n').getBytes()  ); //partialResults[i]
                                    while (b1.remaining() > 0) {
                                        channel.write(b1, position);
                                    }
                                }
                            }
                        } else {
                            System.out.println("Finished line:[" + i + "]");
                        }
                    }
                    /*
                        //writer.writeUTF("-9,"); // Phenotype placeholder.
                        for (int j = 0; j < totalLines; ++j) {
                            if (j < totalLines - 1) {
                                writer.writeUTF(partialResults[j] + ",");
                            } else {
                                writer.writeUTF("" + partialResults[j]);
                            }
                        }
                        writer.writeUTF("\n");
                    */
                    lineScanner.close();
                } else {
                    System.out.println("Null line:[" + i + "]");
                }

            }


            channel.force(true);
        } catch (FileNotFoundException e) {
            System.out.println("There was an error finding a file.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("There was a problem accessing the file.");
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Index does not match expectation. Possible malformed file.");
            e.printStackTrace();
        }
    }

    /* 
     * @param lineNumber    The line number that is being parsed and indexed into partialResults[].
     * @param entry  Is a two character allele entry from an HMP file
     */
    static private void accumulateResults(long lineNumber, String entry) {
        if (entry == null || entry.isBlank() || entry.isEmpty()) {
            System.out.println("The provided entry contained no data.");
            partialResults[(int) lineNumber] = MISSING_DATA + 1;
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
        partialResults[(int) lineNumber] = result;
    }

}
