#!/bin/bash


datasets download genome taxon "$@"
unzip ncbi_dataset*
mkdir assemblies
find ncbi_dataset*/data/* -name '*.*' -type f | xargs mv --target-directory=assemblies
rm -r ncbi_dataset* README.md
