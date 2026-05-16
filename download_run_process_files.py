import boto3
import argparse
from argparse import ArgumentParser


parser = ArgumentParser()
parser.add_argument("--command_run", dest="run",  default=".command.run", help='path/to/.command.run')
parser.add_argument("--outdir", dest="outdir",  default="./", help='workdir of poppunk_assign')

args = parser.parse_args() 
run = args.run
outdir = args.outdir.rstrip("/")

s3_client = boto3.client('s3')

def get_relevant_filepaths(filename):
    with open(filename, "r") as f:
        kept = [line.strip().split("nxf_s3_download ")[-1].split(' ')[0] for line in f if 'downloads+=("nxf_s3_download s3://' in line]
    # uncomment to remove .command files
    # kept = [s for s in kept if "/." not in s]
    return kept


def get_s3_download_input(s3_path: str) -> list:
    """ 
    Parse an S3 path to retrieve the bucket and key. This function takes an S3 
    path, strips the 's3://' prefix, and splits the remaining path into the S3 
    bucket name and the S3 key. The bucket name and key are then returned as a 
    list. 
    Args: 
        s3_path (str): The full S3 path in the format 's3://bucket_name/key'. 
    Returns: 
        list: A list containing the S3 bucket name and the S3 key.
    """
    s3_path = s3_path.replace("s3://", "")
    s3_split = s3_path.split("/")
    s3_bucket = s3_split[0]
    s3_key = "/".join(s3_split[1:])
    local = s3_split[-1]
    return s3_bucket, s3_key, local



cleaned = get_relevant_filepaths(run)
for pth in cleaned:
    launch_bucket, path, base = get_s3_download_input(pth)
    s3_client.download_file(launch_bucket, path, f"{outdir}/{base}")
    print(f"downloaded {outdir}/base")
