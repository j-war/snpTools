Converter ReadMe:

The purpose of this program is to allow the conversion of biological Single Nucleotide Polymorphisms (SNPs) data between common data formats. Formats include plink (.ped, .map) to csv, vcf to csv, and hapmap (.hmp) to csv.
SNPs are records of changes in single DNA base pairs and are often stored in a variety of file formats depending on the source aond configuration settings. Conversion between formats is neccessary to take advantage of the many analysis tools available today that only support certain data formats.

Conversion currently only supports diploid cells.
The main entry point is DataInput.java, it creates and starts the conversion process according to the mode argument.

Explanation of csv output file:
Each sample of input data is represented as a single line in the csv output file.
The first value of a line is the determined phenotype based on the data contained in the input file. A "-9" is used as a default placeholder value if no data could be found. This value is also use in PLINK software to designate a missing value for the phenotype.
The rest of the entries on a line are the SNPs for that sample. 
Explanation of values:
0:
1:
2:

The general flow of conversion:
-determine file layout and constraints
-normalize data by dividing and distributing work
-collect results
-calculate intermediate data
-analyze input
-merge results
-cleanup of temporary files

// Flow diagrams for conversion algorithms



All from the Converter root directory:

Compiling:
javac -d [Destination directory relative to current directory] Main.java

Examples:
javac -d ./Converter DataInput.java
javac -d Converter DataInput.java


Running:
java -cp [Classpath directory] Main [mode] [Relative input filepath with an extension] [Relative output filepath with an extension]

Valid modes:
0 for ped->csv
1 for vcf->csv
2 for hmp->csv

Examples:
java -cp ./Converter DataInput 0 ./InputFolder/mdp_genotype.plk ./OutputFolder/OutputPed
java -cp Converter DataInput 0 InputFolder/mdp_genotype.plk OutputFolder/OutputPed


Compiling and running:
Examples:
javac -d ./Converter DataInput.java && java -cp ./Converter DataInput 0 ./InputFolder/mdp_genotype.plk ./OutputFolder/OutputPed
javac -d ./Converter DataInput.java && java -cp ./Converter DataInput 1 ./InputFolder/mdp_genotype ./OutputFolder/OutputVcf
javac -d ./Converter DataInput.java && java -cp ./Converter DataInput 2 ./InputFolder/mdp_genotype ./OutputFolder/OutputHmp

Or:
javac -d Converter DataInput.java && java -cp Converter DataInput 0 InputFolder/mdp_genotype.plk OutputFolder/OutputPed
javac -d Converter DataInput.java && java -cp Converter DataInput 1 InputFolder/mdp_genotype OutputFolder/OutputVcf
javac -d Converter DataInput.java && java -cp Converter DataInput 2 InputFolder/mdp_genotype OutputFolder/OutputHmp



Folders:

java -cp /home/user/myprogram org.mypackage.HelloWorld

/home/user/myprogram/
            |
            ---> org/
                  |
                  ---> mypackage/
                           |
                           ---> HelloWorld.class
                           ---> SupportClass.class
                           ---> UtilClass.class
