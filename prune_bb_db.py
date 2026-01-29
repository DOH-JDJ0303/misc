#!/usr/bin/env python3
"""
find_sample_files_in_s3.py

Scan S3 clusters directory for sample files (.tar.gz and .fa.gz).
Identifies files matching sample IDs from input CSV and outputs
files to discard/keep to output CSV.
"""

import argparse
import csv
import logging
import sys
from typing import Dict, List, Set

import boto3


def setup_logging(verbose: bool = False) -> None:
    """Configure logging output."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and prefix."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Not an S3 URI: {s3_uri}")
    
    path = s3_uri[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) == 2 else ""
    
    # Ensure prefix ends with /
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    
    return bucket, prefix


def load_sample_ids(csv_path: str, column: str = None) -> Set[str]:
    """Load sample IDs from CSV file, returning unique set."""
    sample_ids = set()
    
    logging.info(f"Loading sample IDs from: {csv_path}")
    
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        if column:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or column not in reader.fieldnames:
                available = reader.fieldnames or []
                raise ValueError(f"Column '{column}' not found. Available: {available}")
            
            for row in reader:
                sid = (row.get(column) or "").strip()
                if sid:
                    sample_ids.add(sid)
        else:
            reader = csv.reader(fh)
            for row in reader:
                if row:
                    sid = str(row[0]).strip()
                    if sid:
                        sample_ids.add(sid)
    
    logging.info(f"Loaded {len(sample_ids)} unique sample IDs")
    return sample_ids


def scan_s3_cluster_files(bucket: str, prefix: str, sample_ids: Set[str]) -> Dict[str, any]:
    """
    Scan S3 clusters directory and categorize files.
    
    Returns dict with keys:
        - 'files_to_discard': files matching sample IDs
        - 'files_to_keep': assembly/snippy files NOT matching sample IDs
        - 'found_samples': dict tracking which sample IDs were found with assembly/snippy
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    
    clusters_prefix = f"{prefix}clusters/"
    logging.info(f"Scanning S3: s3://{bucket}/{clusters_prefix}")
    
    # Track files by cluster and category
    cluster_data: Dict[str, Dict[str, List[str]]] = {}
    
    # Track which sample IDs have assembly and snippy files
    # Structure: {sample_id: {'assembly': bool, 'snippy': bool}}
    found_samples: Dict[str, Dict[str, bool]] = {
        sid: {'assembly': False, 'snippy': False} for sid in sample_ids
    }
    
    file_count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=clusters_prefix):
        for obj in page.get("Contents", []):
            file_count += 1
            key = obj["Key"]
            
            # Parse key structure: prefix/clusters/cluster_name/filetype/filename
            parts = key.split('/')
            if len(parts) < 3:
                continue
            
            cluster = parts[-3]
            filetype = parts[-2]
            filename = parts[-1]
            
            if not filename:
                continue
            
            # Initialize cluster data structure
            if cluster not in cluster_data:
                cluster_data[cluster] = {
                    'discard': [],
                    'keep': [],
                    'other': []
                }
            
            # Categorize based on filetype
            if filetype in ['assembly', 'snippy']:
                # Extract sample ID from filename
                if filename.endswith('.tar.gz'):
                    sid = filename[:-7]  # Remove .tar.gz
                elif filename.endswith('.fa.gz'):
                    sid = filename[:-6]  # Remove .fa.gz
                else:
                    continue
                
                # Check if sample ID matches
                if sid in sample_ids:
                    cluster_data[cluster]['discard'].append(key)
                    logging.debug(f"Discard: {key} (matches sample {sid})")
                    
                    # Track that we found this sample ID with this filetype
                    found_samples[sid][filetype] = True
                else:
                    cluster_data[cluster]['keep'].append(key)
            else:
                # Other files in cluster (metadata, etc.)
                cluster_data[cluster]['other'].append(key)
    
    logging.info(f"Scanned {file_count} total files across {len(cluster_data)} clusters")
    
    # Compile final output lists
    all_files_to_discard = []
    all_files_to_keep = []
    
    for cluster, files in cluster_data.items():
        all_files_to_discard.extend(files['discard'])
        all_files_to_keep.extend(files['keep'])
        
        # If no files to keep in this cluster, remove everything including 'other' files
        if not files['keep'] and files['other']:
            logging.info(f"Cluster '{cluster}': All samples discarded, removing {len(files['other'])} additional files")
            all_files_to_discard.extend(files['other'])
        # If there are files to keep, also keep the 'other' files in this cluster
        elif files['keep'] and files['other']:
            logging.info(f"Cluster '{cluster}': Keeping {len(files['keep'])} sample files and {len(files['other'])} other files")
            all_files_to_keep.extend(files['other'])
        elif files['discard'] and not files['keep']:
            logging.info(f"Cluster '{cluster}': Discarding {len(files['discard'])} files (no files to keep)")
        elif files['discard']:
            logging.info(f"Cluster '{cluster}': Discarding {len(files['discard'])} files, keeping {len(files['keep'])} files")
    
    logging.info(f"Total files to discard: {len(all_files_to_discard)}")
    logging.info(f"Total files to keep: {len(all_files_to_keep)}")
    
    return {
        'files_to_discard': all_files_to_discard,
        'files_to_keep': all_files_to_keep,
        'found_samples': found_samples
    }


def write_output_csv(output_path: str, files: List[str]) -> None:
    """Write list of files to output CSV."""
    logging.info(f"Writing output to: {output_path}")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['s3_key'])
        
        for file_key in files:
            writer.writerow([file_key])
    
    logging.info(f"Wrote {len(files)} files to output CSV")


def report_missing_files(found_samples: Dict[str, Dict[str, bool]]) -> None:
    """Report sample IDs that are missing assembly or snippy files."""
    missing_assembly = []
    missing_snippy = []
    missing_both = []
    complete = []
    
    for sid, files in found_samples.items():
        has_assembly = files['assembly']
        has_snippy = files['snippy']
        
        if not has_assembly and not has_snippy:
            missing_both.append(sid)
        elif not has_assembly:
            missing_assembly.append(sid)
        elif not has_snippy:
            missing_snippy.append(sid)
        else:
            complete.append(sid)
    
    # Report findings
    print("\n" + "="*70)
    print("SAMPLE FILE STATUS REPORT")
    print("="*70)
    
    print(f"\n✓ Complete (have both assembly and snippy): {len(complete)}")
    
    if missing_both:
        print(f"\n✗ Missing BOTH assembly and snippy files: {len(missing_both)}")
        for sid in sorted(missing_both):
            print(f"  - {sid}")
    
    if missing_assembly:
        print(f"\n⚠ Missing assembly files only: {len(missing_assembly)}")
        for sid in sorted(missing_assembly):
            print(f"  - {sid}")
    
    if missing_snippy:
        print(f"\n⚠ Missing snippy files only: {len(missing_snippy)}")
        for sid in sorted(missing_snippy):
            print(f"  - {sid}")
    
    print("\n" + "="*70)
    print(f"Total samples checked: {len(found_samples)}")
    print(f"Complete: {len(complete)} | Incomplete: {len(missing_assembly) + len(missing_snippy) + len(missing_both)}")
    print("="*70 + "\n")
    
    # Log summary
    if missing_both or missing_assembly or missing_snippy:
        logging.warning(f"{len(missing_both) + len(missing_assembly) + len(missing_snippy)} sample IDs have incomplete files")
    else:
        logging.info("All sample IDs have both assembly and snippy files")



def main() -> int:
    ap = argparse.ArgumentParser(
        description="Identify S3 files in cluster directories matching sample IDs for removal"
    )
    ap.add_argument("--csv", required=True, help="Input CSV with sample IDs")
    ap.add_argument("--column", default=None, help="Column name containing sample IDs (default: first column)")
    ap.add_argument("--s3", required=True, help="S3 prefix to scan (e.g., s3://my-bucket/data/)")
    ap.add_argument("--out", required=True, help="Output CSV path for files to discard")
    ap.add_argument("--keep-out", default=None, help="Output CSV path for files to keep (optional)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    
    args = ap.parse_args()
    
    setup_logging(args.verbose)
    
    try:
        # Parse S3 location
        bucket, prefix = parse_s3_uri(args.s3)
        
        # Load sample IDs
        sample_ids = load_sample_ids(args.csv, args.column)
        
        if not sample_ids:
            logging.error("No sample IDs found in CSV")
            return 2
        
        # Scan S3 and categorize files
        results = scan_s3_cluster_files(bucket, prefix, sample_ids)
        
        # Write discard files
        write_output_csv(args.out, results['files_to_discard'])
        
        # Write keep files if requested
        if args.keep_out:
            write_output_csv(args.keep_out, results['files_to_keep'])
        
        # Report missing files
        report_missing_files(results['found_samples'])
        
        logging.info("Processing complete")
        return 0
        
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    sys.exit(main())