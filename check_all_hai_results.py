import os
import pandas as pd
from argparse import ArgumentParser
import json
from pathlib import Path
import glob
from typing import Iterable, List, Any
import boto3
s3 = boto3.client('s3')


"""
check_phoenix_run_results.py

Purpose
-------
Download a manifest and summary report for a Phoenix workflow run from S3,
compare the expected sample IDs in the manifest with the IDs present in the
summary report, and print which samples (if any) are missing.

Requirements
------------
- Python 3.7+
- boto3 installed and configured with credentials that can access the target S3 bucket
- pandas installed

Behavior
--------
- Downloads two objects from S3 (constructed from the provided run name and
  the 'phoenix' workflow path):
    workflow/phoenix/runs/{run}/manifest.csv
    workflow/phoenix/runs/{run}/Phoenix_Summary.tsv
- Loads the manifest (CSV) and the summary (TSV) into pandas DataFrames.
- Compares manifest['sample'] to report['ID'] and reports unique manifest
  values that are not present in the report, preserving the manifest order.
- Prints "MISSING" followed by missing IDs when there are missing items,
  otherwise prints "COMPLETE".

Usage
-----
From the shell:
    python check_phoenix_run_results.py --bucket my-bucket --run_name 2025-01-01_run123

Arguments
---------
--bucket        (required) S3 bucket name (without "s3://")
--workflow      (optional) workflow name; default "phoenix" (only phoenix is supported)
--run_name      (required) run identifier used to build S3 keys and local filenames

Examples
--------
# Check run "r001" in bucket "my-bucket"
python check_phoenix_run_results.py --bucket my-bucket --run_name r001

Notes and safe defaults
----------------------
- The script currently only constructs S3 keys for the "phoenix" workflow path.
  To support other workflows, add their path logic where the manifest/report
  keys are built.
- Ensure the AWS credentials used by boto3 have read access to the provided bucket.
- Local files named "{run}_manifest.csv" and "{run}_Phoenix_Summary.tsv" will be
  created in the working directory; remove or rotate them if re-running on the
  same run name to avoid confusion.
"""


parser = ArgumentParser()
parser.add_argument("--bucket", dest="bk",  default="", help="bucket name (no 's3://')" )
parser.add_argument("--workflow", dest="wf",  default="phoenix", help="workflow name to check results for" )
parser.add_argument('--run_name', dest = "run", default=None, help="run name " )

args = parser.parse_args() 
bucket = args.bk
workflow = args.wf
run = args.run


def return_missing(manifest: List, report: List) -> List[Any]:
    """Return unique items from manifest not in report, preserving first-seen order."""
    report_set = set(report)
    seen = set()
    missing: List[Any] = []
    for m in manifest:
        if m in report_set or m in seen:
            continue
        seen.add(m)
        missing.append(m)
    return missing


if workflow=="phoenix":
    manifest = f"workflow/phoenix/runs/{run}/manifest.csv"
    report = f"workflow/phoenix/runs/{run}/Phoenix_Summary.tsv"
    s3.download_file(bucket, manifest, f"{run}_manifest.csv")
    s3.download_file(bucket, report, f"{run}_Phoenix_Summary.tsv")

manifest_df = pd.read_csv(f"{run}_manifest.csv", encoding="utf-8")
report_df = pd.read_csv(f"{run}_Phoenix_Summary.tsv", sep="\t", encoding="utf-8")


if workflow=="phoenix":
    manifest_ids = manifest_df['sample'].tolist()
    report_ids = report_df['ID'].tolist()


missing = return_missing(manifest_ids, report_ids)
if missing:
    print("MISSING")
    for i in missing:
        print(i)
else:
    print("COMPLETE")




