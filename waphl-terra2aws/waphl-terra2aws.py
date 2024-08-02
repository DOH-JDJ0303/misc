#!/usr/bin/env python

import sys
import argparse
import pandas as pd
import os
import re
import firecloud.api as fapi
import pathlib
from google.cloud import storage
import boto3
import datetime

#----- ARGUMENTS -----#
parser = argparse.ArgumentParser(
                    prog='waph-terra2aws',
                    description='Transfers data from Terra to AWS')
parser.add_argument('-p',
                    '--project',
                    help = 'Terra billing project name')
parser.add_argument('-w',
                    '--workspace',
                    help = 'Terra workspace name')
parser.add_argument('--target_workflow',
                    help = 'The name of the WDL pipeline/workflow that should be transferred. Files will only be transferred if the workflow name matches this value.')
parser.add_argument('--workflow_name',
                    help = 'The name you would like the workflow to be saved under.')
parser.add_argument('--sample_patterns',
                    nargs = "*",
                    help = 'String patterns that should be extracted from the sample name. Only samples that match one of these pattern will be transferred. Multiple patterns separated by spaces can be supplied (Default: "*").')
parser.add_argument('-u', 
                    '--uri',
                    help = 'URI path to target S3 bucket')
parser.add_argument('-o', 
                    '--outdir',
                    default = 'data',
                    help = 'Directory to save results in the S3 bucket (Default: "data/")')
args = parser.parse_args()

#----- CONFIG PANDAS -----#
pd.set_option("display.max_rows", 1000)
pd.set_option("display.expand_frame_repr", True)
pd.set_option('display.width', 1000)
pd.set_option("display.max_colwidth", 10000)


#----- S3 -----#
if args.outdir.endswith('/'):
    s3_outdir = args.outdir[:-1]
else:
    s3_outdir = args.outdir
    
s3_bucket = args.uri.split('s3://')[1]
if s3_bucket.endswith('/'):
        s3_bucket = s3_bucket[:-1]
s3_client = boto3.client('s3')

#------ DOWNLOAD ALL TABLES FOR WORKSPACE -----#
# set local directory for staging terra tables
local_dir: str = f"terra_tbls/"

# get list of tables in workspace
all_tables = fapi.list_entity_types(args.project, args.workspace).json()
# create directory
pathlib.Path(local_dir).mkdir(parents=True, exist_ok=True)
# download tables
for table in all_tables:
    if '_set' not in table:
        print(f"Downloading table: {table}")
        #os.system("python3 export_large_tsv.py -p "+args.project+" -w "+args.workspace+" -e "+table+" -f "+os.path.join(local_dir,table+".tsv"))

# determine if there are any new tables
new_tables = os.popen(f"aws s3 sync --dryrun --size-only {local_dir} s3://{s3_bucket}/terra_tbls/ | sed 's/(dryrun) upload: //g' | cut -f 1 -d ' '").read().split("\n")

pathlib.Path('tmp').mkdir(parents=True, exist_ok=True)
for table in new_tables[:-1]:
    print(f"Migrating files from table: {table}")
    df = pd.read_csv(table, sep = "\t")
    df.rename(columns={ df.columns[0]: "sample" }, inplace = True)
    gs_cols = ['sample']
    for col in df.columns:
        if "gs://" in df[col].to_string():
            gs_cols.append(col)
    if len(gs_cols) == 1:
        print(f"No Google file paths detected. No files will be migrated")
    else:
        print(f"The following columns will be migrated: {gs_cols[:-1]}")
        sample_name = None
        meta = []
        for index, row in df[gs_cols].iterrows():
            for pattern in args.sample_patterns:
                match = re.search(pattern, row['sample'])
                if match:
                    sample_name = match.group()
            if not sample_name:
                print(f"{row['sample']} does not match any of the supplied patterns ({args.sample_patterns}). Files for this sample will not be transferred.")
            else:
                for col in gs_cols[1:]:
                    gs_path = row[col]
                    if 'gs://' not in gs_path:
                        print(f"No Google file detected in {col}.")
                        file_name = None
                    else:
                        gs_path = gs_path.split('gs://')[1]
                        gs_bucket = gs_path.split('/')[0]
                        gs_blob = '/'.join(gs_path.split('/')[1:])
                        file_name = gs_path.split('/')[-1]

                        if args.target_workflow == gs_path.split('/')[3]:
                            storage_client = storage.Client()
                            bucket = storage_client.bucket(gs_bucket)
                            blob = bucket.get_blob(gs_blob)
                            file_time = blob.time_created.timestamp()
                            blob.download_to_filename(f"tmp/{file_name}")

                            s3_object = f"source={s3_outdir}/sample={sample_name}/workflow={args.workflow_name}/file={file_name}/timestamp={file_time}/{file_name}"
                            s3_uri = f"s3://{s3_bucket}/{s3_object}"
                            s3_client.upload_file(f"tmp/{file_name}", s3_bucket, s3_object)
                    meta_row = [row['sample'], sample_name, file_name, row[col], s3_uri, file_time, "Terra", args.workspace, args.workflow_name]
                    meta_cols = ["ID","ALT_ID","FILE","ORIGIN_PATH","CURRENT_PATH","TIMESTAMP","PLATFORM","WORKSPACE","WORKFLOW"]
                    pd.DataFrame([meta_row], columns=meta_cols).to_csv('tmp/meta.csv', index=False)
                    s3_meta_file = f"{sample_name}_{args.workflow_name}_{file_name}_{file_time}_meta.csv"
                    s3_meta_object = f"meta/{s3_meta_file}"
                    s3_client.upload_file(f"tmp/meta.csv", s3_bucket, s3_meta_object)







                    




