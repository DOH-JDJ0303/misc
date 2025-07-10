#!/usr/bin/env python3

import boto3
import argparse
import textwrap
import sys
from collections import defaultdict

def main():
    version = "1.0"

    parser = argparse.ArgumentParser(description="Create samplesheet from paired reads in an S3 bucket")
    parser.add_argument("bucket_uri", help="URI path to s3 bucket containing reads")
    parser.add_argument('--version', action='version', version=f'%(prog)s {version}')
    args = parser.parse_args()

    print(textwrap.dedent(f""" 
        samplesheet-from-s3.py v{version}
        -----------------------------
    """), flush=True)

    bucket_uri = args.bucket_uri

    if not bucket_uri.startswith('s3://'):
        sys.exit(f'Error: {bucket_uri} does not look like an s3 URI')
    
    bucket_bits = bucket_uri.replace("s3://", "").split('/')
    bucket = bucket_bits[0]
    prefix = '/'.join(bucket_bits[1:])

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        sys.exit("No objects found - check your URI path")
    
    fqs = defaultdict(dict)
    for obj in response['Contents']:
            f = obj['Key']
            fname = f.split('/')[-1]
            ext = fname.split('.')
            if any(e in ['fastq','fq'] for e in ext):
                id = fname.split('_')[0]
                finfo = fname.split('_')[1:]
                if any(e in ['R1','1'] for e in finfo):
                    fqs[id]['r1'] = f
                elif any(e in ['R2','2'] for e in finfo): 
                    fqs[id]['r2'] = f
                else:
                    sys.exit(f"Error: Cannot determine if {fname} is foward or reverse read.")
    
    fq_csv = ['sample,fastq_1,fastq_2']
    for k, v in fqs.items():
        r1 = f"s3://{bucket}/{v['r1']}"
        r2 = f"s3://{bucket}/{v['r2']}"
        fq_csv.append(','.join([str(k),str(r1),str(r2)]))
    
    fileout = 'samplesheet.csv'
    with open(fileout, 'w') as out:
        out.write('\n'.join(fq_csv))

    print(f"File saved to {fileout}")
        

    

if __name__ == "__main__":
    main()