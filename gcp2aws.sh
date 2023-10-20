#!/bin/bash

manifest=$1
aws_bucket=${2%/}

echo "sample,taxa,assembly,fastq_1,fastq_2" > tmp

for line in $(tail -n +2 $manifest)
do
    # split fields
    id=$(echo ${line} | cut -f 1 -d ',' | tr -d '\n\t\r ')
    taxa=$(echo ${line} | cut -f 2 -d ',' )
    assembly_path=$(echo ${line} | cut -f 3 -d ',' | tr -d '\n\t\r ')
    fastq_1_path=$(echo ${line} | cut -f 4 -d ',' | tr -d '\n\t\r ')
    fastq_2_path=$(echo ${line} | cut -f 5 -d ',' | tr -d '\n\t\r ')

    # extract files names
    assembly=${assembly_path##*/}
    fastq_1=${fastq_1_path##*/}
    fastq_2=${fastq_2_path##*/}

    # download files
    echo -e "\n" && gsutil cp ${assembly_path} ./
    echo -e "\n" && gsutil cp ${fastq_1_path} ./
    echo -e "\n" && gsutil cp ${fastq_2_path} ./

    # push to AWS
    echo -e "\n" && aws s3 cp ${assembly} ${aws_bucket}/assemblies/${assembly} 
    echo -e "\n" && aws s3 cp ${fastq_1} ${aws_bucket}/reads/${fastq_1}
    echo -e "\n" && aws s3 cp ${fastq_2} ${aws_bucket}/reads/${fastq_2}

    wait

    # remove intermediates
    rm ${assembly} ${fastq_1} ${fastq_2}

    echo -e "${id},${taxa},${aws_bucket}/assemblies/${assembly},${aws_bucket}/reads/${fastq_1},${aws_bucket}/reads/${fastq_2}" >> tmp
done

aws s3 cp tmp ${aws_bucket}/manifest.csv

