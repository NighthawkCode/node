#!/bin/bash
# this tells bash to stop the script if anything breaks
set -e

rm -fr build
mkdir build
cd build
cmake ..
make
cd ..

./test.sh
