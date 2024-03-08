#!/bin/bash


datasets download genome taxon "$@"
unzip ncbi_dataset*
mkdir assemblies
mv ncbi_dataset/data/*/* assemblies
rm -r ncbi_dataset* README.md
