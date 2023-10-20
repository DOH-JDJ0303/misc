#!/bin/bash

path=${1%/}

aws s3 cp ${path}/Phoenix_Summary.tsv ./

echo "sample,taxa,assembly,fastq_1,fastq_2" > manifest.csv

for i in $(cat Phoenix_Summary.tsv | tr '\t' '@' | tr ' ' '_')
do
    id=$(echo ${i} | cut -f 1 -d '@')
    qc=$(echo ${i} | cut -f 2 -d '@')
    taxa=$(echo ${i} | cut -f 9 -d '@')

    echo -e "${id}\t${qc}\t${taxa}"

    if [[ ${qc} == "PASS"  ]]
    then
        echo "${id},${taxa},${path}/assembly/${id}/${id}.scaffolds.fa.gz,${path}/${id}/fastp_trimd/${id}_1.trim.fastq.gz,${path}/${id}/fastp_trimd/${id}_2.trim.fastq.gz" >> manifest.csv
    fi
done
