#!/bin/zsh

javac -cp ".:lib/*" -d build $(find ./src/main -name "*.java")
mkdir build
cd build
jar cvf DataInput.jar *
cd ..

# Run from snpTools/ directory with:
# ./build.sh

# Remember to clear the contents of build/ if you run this again...
