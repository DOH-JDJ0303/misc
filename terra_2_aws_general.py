#!/usr/bin/env python

import os
import argparse
import firecloud.api as fapi
import subprocess
from argparse import ArgumentParser
from pathlib import Path
import pandas as pd
from typing import List

parser = ArgumentParser()
parser.add_argument("-f", dest="access_file", required=True,
                    help="See template. Contains access info for Terra.bio including GCS bucket, Workspace, and Project Billing. Contact your Terra.bio admin for further assistance")
#parser.add_argument("--clusters", action=argparse.BooleanOptionalAction, help="Adds in recent cluster info from bigbacter") 
parser.add_argument("--tables", dest="tables",  required=True, help="Table(s) to pull and/or push to s3 bucket", nargs="*")
parser.add_argument('--pull', dest="pull", action='store_true', help="Pulls table(s) from Terra.bio")
parser.add_argument("--push", dest="push",  action='store_true', help="Pushes table(s) to s3 bucket")
parser.add_argument("--clean", dest="clean",  action='store_true', help="Pushes table(s) to s3 bucket")


args = parser.parse_args() 
access = pd.read_csv(args.access_file, sep='\t')
project_billing = access["project_billing"].tolist()[0]
terra_workspace = access["workspace"].tolist()[0]
terra_bucket = access["bucket"].tolist()[0]
out_s3 = access["dest_s3"].tolist()[0]
pull = args.pull
push = args.push
tables = args.tables
clean = args.clean
python_v = "python3"
home_dir = str(Path.home())


def local_to_s3(local_file: str,
                s3_file: str = out_s3,
                force: bool = False) -> None:
      if force == False:
            import httplib2
            h = httplib2.Http()
            resp = h.request(s3_file, 'HEAD')
            if int(resp[0]['status']) < 400:
                  subprocess.run(f'aws s3 cp {local_file} {s3_file}', shell=True)
            else:
                  print(s3_file+" already exists")
      else:
            subprocess.run(f'aws s3 cp {local_file} {s3_file}', shell=True)


def script_check(expt: str = f"{home_dir}/terra_aws_scripts/export_large_tsv.py",
                impt: str = f"{home_dir}/terra_aws_scripts/import_large_tsv.py") -> List[str]:
    if not os.path.exists(f'{home_dir}/terra_aws_scripts'):
        print('Path ~/terra_aws_scripts does not yet exist\nCreating ~/terra_aws_scripts ...')
        os.makedirs(f'{home_dir}/terra_aws_scripts')

    if not os.path.exists(expt):  
            print(f"export_large_tsv.py to {home_dir}/terra_aws_scripts/export_large_tsv.py")      
            subprocess.run(f'wget -O {home_dir}/terra_aws_scripts/export_large_tsv.py \
                           https://raw.githubusercontent.com/broadinstitute/terra-tools/master/scripts/export_large_tsv/export_large_tsv.py', 
                           shell=True)
    if not os.path.exists(impt):
            print(f"import_large_tsv.py to {home_dir}/terra_aws_scripts/import_large_tsv.py")      
            subprocess.run(f'wget -O {home_dir}/terra_aws_scripts/import_large_tsv.py \
                           https://raw.githubusercontent.com/broadinstitute/terra-tools/master/scripts/import_large_tsv/import_large_tsv.py', 
                           shell=True)
    
    return expt, impt


def pull_terra_table(project_billing: str,
                     terra_workspace: str,
                     table_name: str,
                     force: bool = True) -> None:
      subprocess.run(f'{python_v} {home_dir}/terra_aws_scripts/export_large_tsv.py \
                     --project {project_billing} --workspace {terra_workspace} \
                     --entity_type {table_name} \
                     --tsv_filename ./{table_name}.tsv', shell=True)


def clean_by_file(filename) -> None:
     # Clean files by extention
     subprocess.run(f'rm -rf *{filename}', shell=True)
     print(f'{filename} has been removed from local environment')
     

if __name__ == "__main__":
    script_check()
    if type(tables) is not list:
        tables = [tables]
    for table in tables:
        if pull:
            pull_terra_table(project_billing, terra_workspace, table)
        if push:
            local_to_s3(table+".tsv" ,  "s3://"+out_s3+table+".tsv", force = True)
    if clean:
         clean_by_file(table+".tsv")
         