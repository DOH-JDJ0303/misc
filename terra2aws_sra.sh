#!/bin/bash

BIOSAMPLEOBJECTS=$1
MERGEDDF=$2
s3=${3%/}

echo "WGS_ID,REASON" > error.txt
echo "biosample_accession,library_ID,title,library_strategy,library_source,library_selection,library_layout,platform,instrument_model,design_description,file_type,filename,filename2" > sra_metadata.csv

# get WGS IDs and SAMN numbers of samples that have been submitted
for line in $(cat $BIOSAMPLEOBJECTS | cut -f 1,2 | tr '\t' ',')
do
    SAMN=$(echo $line | cut -f 1 -d ',')
    WGS_ID=$(echo $line | cut -f 2 -d ',')

    # extract read paths from merged_df
    R1=$(cat $MERGEDDF | tr -d ', ' | cut -f 55,118 | awk -v id=${WGS_ID} '$2 == id {print $1}')
    R2=$(cat $MERGEDDF | tr -d ', ' | cut -f 58,118 | awk -v id=${WGS_ID} '$2 == id {print $1}')

    # sanity check
    status=true
    if [[ "$SAMN" != "SAMN"* ]]
    then
        echo "${WGS_ID},bad_samn" >> error.txt
        status=false
    fi
    if [[ "$WGS_ID" != *"-PHL-"* ]]
    then
        echo "${WGS_ID},bad_id" >> error.txt
        status=false
    fi 
    if [[ "$R1" != "gs://"* ]]
    then
        echo "${WGS_ID},bad_read1_path" >> error.txt
        status=false
    fi
    if [[ "$R2" != "gs://"* ]]
    then
        echo "${WGS_ID},bad_read2_path" >> error.txt
        status=false
    fi
    if [[ "$R1" != *"R1_dehosted.fastq.gz" ]]
    then
        echo "${WGS_ID},bad_read1_file" >> error.txt
        status=false
    fi
    if [[ "$R2" != *"R2_dehosted.fastq.gz" ]]
    then
        echo "${WGS_ID},bad_read2_file" >> error.txt
        status=false
    fi
    if [[ $status == true ]]
    then
        # get basename of read files
        R1_base=$(basename $R1)
        R2_base=$(basename $R2)
            
        # copy files (local)
        gsutil cp $R1 ./
        gsutil cp $R2 ./

        # determine the sequencer
        case $(zcat $R1_base | head -n1 | sed 's/:.*//g' | cut -c 2) in
            "V")
                SEQUENCER="NextSeq 2000"
                ;;
            "M")
                SEQUENCER="Illumina MiSeq"
                ;;
            *)
                SEQUENCER="Unknown"
                ;;
        esac

        # copy files (AWS)
        aws s3 cp $R1_base $s3/${WGS_ID}_R1.fastq.gz && rm $R1_base
        aws s3 cp $R2_base $s3/${WGS_ID}_R2.fastq.gz && rm $R2_base

        # check if reads made it to AWS
        R1_check=true && aws s3 ls $s3/${WGS_ID}_R1.fastq.gz || R1_check=false
        R2_check=true && aws s3 ls $s3/${WGS_ID}_R2.fastq.gz || R2_check=false

        if [[ $R1_check == true ]] && [[ $R2_check == true ]]
        then
            # create NCBI metadata file
            echo "$SAMN,$WGS_ID,Baseline surveillance (random sampling) of severe acute respiratory syndrome coronavirus 2,WGS,VIRAL RNA,PCR,paired,ILLUMINA,$SEQUENCER,Tiled-amplicon whole genome sequencing of severe acute respiratory syndrome coronavirus 2,fastq,$R1,$R2" >> sra_metadata.csv
        else
            echo ${WGS_ID},no_file_aws >> error.txt
        fi
    fi
done