import boto3
from argparse import ArgumentParser
from typing import List
import glob
import pandas as pd
import os


"""
Script to fix manifest+phoenix_summary.tsv 
overwrite that occurs when a single sequencing run 
is split up into two or more bioinformatics processing batches. 
 
If your Phoenix run is located at s3://<bucket>/workflow/phoenix/runs/<run_name>/,
run this script with:
python --prefix workflow/phoenix/runs/<run_name> --bucket <bucket>
"""


parser = ArgumentParser()
parser.add_argument("--prefix", dest="prefix", help="path/phoenix/run")
parser.add_argument("--bucket", dest="bucket", help="path/phoenix/run")

args = parser.parse_args() 
prefix = args.prefix
bucket_name = args.bucket

s3_client = boto3.client('s3')


def list_s3_objects(bucket_name, prefix=""):
    """
    List all objects in an S3 bucket with an optional prefix.

    :param bucket_name: Name of the S3 bucket
    :param prefix: (Optional) Prefix to filter objects by path
    """
    
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    objs = []
    #print(f"Listing objects in bucket: {bucket_name}")
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                #print(obj['Key'])  # Print the object key (path)
                objs.append(obj['Key']) 
    return objs


def get_phoenix_reads(objs):
    sample_list = get_phoenix_samples(objs)
    
    sample_reads = {}
    sample_summary = []
    for sample in sample_list:
        # Get fastq files
        fastqs = [f"s3://{bucket_name}/{item}" for item in objs if f"/reads/{sample}" in item]
        sample_reads[sample] = fastqs
        sample_summary.append(f"{prefix}/{sample}/{sample}_summaryline.tsv")
    
    return sample_reads, sample_summary


def make_manifest_csv(sample_reads):
    df = pd.DataFrame.from_dict(sample_reads, orient='index', columns=['fastq_1', 'fastq_2'])

    # Reset index to make keys a proper column (optional)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'sample'}, inplace=True)
    df.to_csv("manifest.csv", index=False)


def make_phoenix_summary(sample_summary,_temp_files):
    for s3_file_key in sample_summary:
        local = s3_file_key.split("/")[-1]
        temp_files.append(local)
        s3_client.download_file(bucket_name, s3_file_key, local)

    tsv_files = glob.glob("WA*.tsv")
    merged_df = pd.concat([pd.read_csv(file, sep='\t') for file in tsv_files], ignore_index=True)

    # Save the merged table to a new TSV file
    merged_df.to_csv("Phoenix_Summary.tsv", sep='\t', index=False)
    return temp_files


def get_phoenix_samples(objs):
    sample_list = []
    for obj in objs:
        if obj != prefix+"/" :
            if obj.endswith("/"):
                if obj[-2].isdigit():
                    sample_list.append(obj.split("/")[-2])
    return sample_list


if __name__ == "__main__":
    li = list_s3_objects(bucket_name, prefix)
    #print(li)
    sample_list = get_phoenix_samples(li)
    sample_reads, sample_summary = get_phoenix_reads(li)

    # Make and upload Phoenix_summary.tsv and manifest.csv files
    temp_files = ["Phoenix_Summary.tsv", "manifest.csv"]

    temp_files = make_phoenix_summary(sample_summary, temp_files)
    s3_client.upload_file("Phoenix_Summary.tsv", bucket_name, f"{prefix}/Phoenix_Summary.tsv")

    make_manifest_csv(sample_reads)
    s3_client.upload_file("manifest.csv", bucket_name, f"{prefix}/manifest.csv")

    # Remove files related to summary and manifest
    for file_path in temp_files:
        os.remove(file_path)