#!/bin/zsh

mode=$1
inputFile=$2
outputFile=$3

java -cp target/converter-1.0.jar com.snptools.converter.DataInput ${mode} ${inputFile} ${outputFile}

# Example:
# java -cp target/converter-1.0.jar com.snptools.converter.DataInput 2 ./InputFolder/mdp_genotype.hmp ./OutputFolder/OutputHmpToCsv.csv

# Run from snpTools/ directory with:
# ./mvnrun.sh 2 ./InputFolder/InputFile.hmp ./OutputFolder/OutputFile.csv
