#!/bin/bash


# Create MASH sketch
threads="$1"
find reference_genomes -name '*.fna' -type f | xargs mash sketch -o reference 
find assemblies -name '*.fna' -type f | xargs mash sketch -o samples
mash dist reference.msh samples.msh -p $threads | awk -v OFS='\t' '{print $1, $2, 100*(1-$3)}' > results.txt