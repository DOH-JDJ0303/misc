#!/bin/bash


# Create sig files
find reference_genomes -name '*.fna*' -type f | xargs sourmash sketch dna -p k=21,k=31,k=51
mv *sig reference_genomes/

find assemblies -name '*.fna*' -type f | xargs sourmash sketch dna -p k=21,k=31,k=51
mv *sig assemblies/