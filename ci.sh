#!/bin/bash
# this tells bash to stop the script if anything breaks
set -e

bazel build --verbose_failures ...

./test.sh
