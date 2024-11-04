#!/bin/bash

AWS_PATH=${1%/}

aws s3 cp ${AWS_PATH}/Phoenix_Summary.tsv ./

echo "sample,taxa,assembly,fastq_1,fastq_2" > manifest.csv

for i in $(cat Phoenix_Summary.tsv | tr '\t' '@' | tr ' ' '_')
do
    ID=$(echo ${i} | cut -f 1 -d '@'); ID=${ID%%-WAPHL*}
    QC=$(echo ${i} | cut -f 2 -d '@')
    TAXA=$(echo ${i} | cut -f 9 -d '@')

    echo -e "${ID}\t${QC}\t${TAXA}"

    if [[ ${QC} == "PASS"  ]]
    then
        echo "${ID},${TAXA},${AWS_PATH}/${ID}/assembly/${ID}.scaffolds.fa.gz,${AWS_PATH}/${ID}/fastp_trimd/${ID}_1.trim.fastq.gz,${AWS_PATH}/${ID}/fastp_trimd/${ID}_2.trim.fastq.gz" >> manifest.csv
    fi
done
