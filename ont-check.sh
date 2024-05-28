#!/bin/bash
message () {
    echo -e "\033[0;36m$1\033[0m"
}

message "ont-check.sh v1.0\n"

#----- HELP -----#
[ "$1" == '' ] || [ "$1" == '-h' ] || [ "$1" == '--help' ] || [ "$1" == '-help' ] && { message "Example: ont-check.sh [URI path] [Reference path]\n\nURI Path:\t AWS URI path to ONT run directory\nReference path:\tLocal path to reference genome."; exit 0; }

set -euo pipefail

#---- INPUTS ----#
RUN_DIR="${1%/}/"
REF=$2

#----- CHECK INPUT -----#
aws s3 ls "$RUN_DIR" > /dev/null 2>&1 || { echo "ERROR: ${RUN_DIR} is not a valid URI path"; exit 1; }
cat $REF | sed -n 1p | grep -q '>' || { echo "ERROR: There is a problem with ${REF}"; exit 1; }
message "INPUTS:\nURI path: ${RUN_DIR}\nReference: ${REF}\n"

#----- CONFIG -----#
RD=$(basename "$RUN_DIR")
ID=$(echo "$RD" | tr ' ' '_')
mkdir ${ID} > /dev/null 2>&1 || true
cd ${ID}
mkdir reads > /dev/null 2>&1 || true
cp ../"$REF" ./
REF=$(basename "$REF")
message "Saving results to ${ID}/"

#---- PREPARING READS ----#
# Download reads
message "Downloading reads..."
RD_HASH=$(aws s3 ls "$RUN_DIR" | grep 'PRE' | sed 's/.*PRE //g')
FQP="${RUN_DIR}${RD_HASH}fastq_pass/"
aws s3 ls "${FQP}" > /dev/null 2>&1 || { echo "ERROR: ${FQP} is not a valid URI path"; exit 1; }
aws s3 sync "${FQP}" reads/

# Combine reads
message "Combining reads..."
cat reads/*.fastq.gz > all.fastq.gz

#----- DETERMINE TAXONOMY ----#
# check for sourmash database
if [ -f gtdb-rs214-reps.k31.zip ]
then
    message "Sourmash database detected. Skipping..."
else
   message "Downloading sourmash database..."
   wget https://farm.cse.ucdavis.edu/~ctbrown/sourmash-db/gtdb-rs214/gtdb-rs214-reps.k31.zip || true
fi

# generate sketch file for reads
if [ -f all.fastq.gz.sig ]
then
    message "Skecth file detected. Skipping..."
else
    message "Sketching reads..."
    podman run --rm -v $PWD:/data/ biocontainers/sourmash:4.8.4--hdfd78af_0 sourmash sketch dna --outdir /data/ /data/all.fastq.gz
fi
# determine taxonomy
if [ -f "${RD}.sm-out.csv" ]
then
    message "Taxonomy file detected. Skipping..."
else
    message "Predicting taxonomy..."
    podman run --rm -v $PWD:/data/ biocontainers/sourmash:4.8.4--hdfd78af_0 sourmash gather -o /data/"${ID}.sm-out.csv" /data/all.fastq.gz.sig /data/gtdb-rs214-reps.k31.zip > "${ID}.sm-summary.csv"
fi

#----- DE NOVO ASSEMBLE -----#
# run Flye assembler
if [ -f ${ID}.fa ]
then
    message "Assembly file detected. Skipping..."
else
    message "Assembling genome..."
    podman run --rm -v $PWD:/data/ docker.io/staphb/flye:2.9.4 flye -t 8 --nano-hq /data/all.fastq.gz -o /data/flye
    cp flye/assembly.fasta ${ID}.fa
fi
# compare de novo assembly to reference
if [ -f ${ID}_quast.txt ]
then
    message "Quast results detected. Skipping..."
else
    message "Comparing to reference..."
    podman run --rm -v $PWD:/data/ docker.io/staphb/quast:5.2.0 quast.py -t 8 --nanopore all.fastq.gz -r "/data/$REF" -o /data/quast /data/flye/assembly.fasta
    cp quast/report.txt ${ID}_quast.txt
fi

#----- DONE -----#
message "Done!"