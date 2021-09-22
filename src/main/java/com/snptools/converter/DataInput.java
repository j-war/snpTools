package com.snptools.converter;

import com.snptools.converter.hmputilities.HmpController;
import com.snptools.converter.pedutilities.PedController;
import com.snptools.converter.vcfutilities.VcfController;
import com.snptools.converter.fileutilities.FileController;

/**
 * DataInput is the main entry point for the Converter java program.
 * This program will attempt to convert the provided .ped, .vcf, or .hmp
 * file into a .csv file.
 * Conversion between .vcf and .hmp files is also possible.
 *
 * Note: Conversion between .vcf and .hmp results in the loss of some information
 *       in the destination format. In particular, .vcf meta data is not included
 *       in the .hmp file format.
 * @author  Jeff Warner
 * @version 1.2, August 2021
 */
class DataInput {

    public static void main(String[] args) {

        final int NUMBER_OF_ARGS = 3; // The expected number of inputs to begin processing.

        long startTime = System.nanoTime(); // Init.
        long endTime = System.nanoTime(); // Init.

        // START:
        startTime = System.nanoTime();
        if (args.length == NUMBER_OF_ARGS) {
            processArguments(args);
        } else {
            System.out.println("Please provide " + NUMBER_OF_ARGS + " arguments consisting of the mode of operation (0: ped->csv, 1: vcf->csv), the name of the input file, and the name of the output file.");
        }

        // END:
        endTime = System.nanoTime();

        // RESULT:
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);

    }

    /**
     * Process and direct computation based on provided arguments.
     * @param args  The array of string arguments where the first entry in the array
     *              should be an integer to indicate the desired conversion mode.
     */
    private static void processArguments(String[] args) {
        int mode = -1;
        try {
            mode = Integer.parseInt(args[0]);
        } catch (NumberFormatException exception) {
            System.out.println("Please provide an integer as the mode of operation (0: ped->csv, 1: vcf->csv, etc.), the name of the input file, and the name of the output file.");
            return; // Invalid input arguments, exit.
        }

        if (!canAccessDataFiles(args[1], args[2])) {
            System.out.println("Error: Unable to find or access input or output locations.");
            return; // Unable to access a file/Missing file, exit.
        }

        switch (mode) {
            case 0: { // ped->csv
                System.out.println("Mode 0: Attempting to convert .ped to .csv");
                PedController pedController = new PedController(args[1], args[2]);
                pedController.startPedToCsv();
            }
                break;
            case 1: { // vcf->csv
                System.out.println("Mode 1: Attempting to convert .vcf to .csv");
                VcfController vcfController = new VcfController(args[1], args[2]);
                vcfController.startVcfToCsv();
            }
                break;
            case 2: { // hmp->csv
                System.out.println("Mode 2: Attempting to convert .hmp to .csv");
                HmpController hmpController = new HmpController(args[1], args[2]);
                hmpController.startHmpToCsv();
            }
                break;
            case 3: { // vcf->hmp
                System.out.println("Mode 3: Attempting to convert .vcf to .hmp");
                VcfController vcfController = new VcfController(args[1], args[2]);
                vcfController.startVcfToHmp();
            }
                break;
            case 4: { // hmp->vcf
                System.out.println("Mode 4: Attempting to convert .hmp to .vcf");
                HmpController hmpController = new HmpController(args[1], args[2]);
                hmpController.startHmpToVcf();
            }
                break;
            default: { // Unknown mode.
                System.out.println("Unknown mode.\nPlease provide the mode of operation (0: ped->csv, 1: vcf->csv, 2: hmp->csv, 3: vcf->hmp, 4: hmp->vcf), the name of the input file, and the name of the output file.");
                break;
            }
        }
    }

    /**
     * Use as an early check to determine the existence of the input file and
     * output folder.
     * 
     * @return  Whether the set input file and output path are accessible.
     */
    public static boolean canAccessDataFiles(String inputFile, String outputFile) {
        if (!FileController.canReadFile(inputFile)) {
            System.out.println("\nError: Cannot read input file, closing.\n");
            return false;
        } // else { System.out.println("Can read input file, continuing."); }

        if (!FileController.directoryExists(outputFile)) {
            System.out.println("\nError: Output folder is missing, closing.\n");
            return false;
        } // else { System.out.println("Can read output folder"); }

        if (FileController.isADirectory(inputFile)) {
            System.out.println("\nError: Designated input file is a folder, closing.\n");
            return false;
        }

        if (FileController.isADirectory(outputFile)) {
            System.out.println("\nError: Designated output file is a folder, closing.\n");
            return false;
        }

        return true;
    }

}


/*

jar tf target/converter-1.0.jar
java -cp target/converter-1.0.jar com.snptools.converter.DataInput 0 ./InputFolder/mdp_genotype.plk ./OutputFolder/OutputPedToCsv


mvn package && java -cp target/converter-1.0.jar com.snptools.converter.DataInput 0 ./InputFolder/mdp_genotype.plk.ped ./OutputFolder/OutputPedToCsv.csv

mvn package && java -cp target/converter-1.0.jar com.snptools.converter.DataInput 1 ./InputFolder/mdp_genotype.vcf ./OutputFolder/OutputVcfToCsv.csv

mvn package && java -cp target/converter-1.0.jar com.snptools.converter.DataInput 2 ./InputFolder/mdp_genotype.hmp ./OutputFolder/OutputHmpToCsv.csv

mvn package && java -cp target/converter-1.0.jar com.snptools.converter.DataInput 3 ./InputFolder/mdp_genotype.vcf ./OutputFolder/OutputVcfToHmp.hmp

mvn package && java -cp target/converter-1.0.jar com.snptools.converter.DataInput 4 ./InputFolder/mdp_genotype.hmp ./OutputFolder/OutputHmpToVcf.vcf

*/
