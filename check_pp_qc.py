import pickle, numpy as np
import pandas as pd
from scipy.spatial.distance import squareform
import argparse
from argparse import ArgumentParser
from io import StringIO
import boto3
import os
import subprocess
import pickle
import numpy as np
import sys
from pathlib import Path

# Source - https://stackoverflow.com/a/973488
# Posted by Blair Conrad, modified by community. See post 'Timeline' for change history
# Retrieved 2026-05-19, License - CC BY-SA 3.0

# print(next(os.walk('.'))[1])
# exit(1)


parser = ArgumentParser()
parser.add_argument("--bb", dest="bb", help='bb_input')
parser.add_argument("--species_path", dest="sp", help='s3 path of bb species dbs')
parser.add_argument("--workdir", dest="workdir", help='workdir of poppunk_assign')
parser.add_argument("--output", dest="output", default='check_pp_qc_output', help='prefix for output csv')
parser.add_argument("--core_threshold", dest="core", default=0.1, help='Detected Poppunk core genome threshold')
parser.add_argument("--acc_threshold", dest="acc", default=0.6, help='Detected Poppunk accessory genome threshold')


args = parser.parse_args() 
bb = args.bb
sp = args.sp
core = args.core
acc = args.acc
output = args.output
run = args.workdir+".command.run"
sh = args.workdir+".command.sh"


s3_client = boto3.client('s3')


# Functions
def get_relevant_filepaths(filename):
    with open(filename, "r") as f:
        kept = [line.strip().split("nxf_s3_download ")[-1].split(' ')[0] for line in f if 'downloads+=("nxf_s3_download s3://' in line]
    # uncomment to remove .command files
    # kept = [s for s in kept if "/." not in s]
    return kept


def list_s3_objects(s3_uri: str) -> list:
    """
    List all objects under a given S3 directory-like prefix.
    
    Args:
        s3_uri (str): Full S3 URI (e.g., 's3://my-bucket/path/to/folder/')
    
    Returns:
        list: List of object keys under the prefix.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'")

    # Parse bucket and prefix
    parts = s3_uri[5:].split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    objects = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                objects.append(obj['Key'])

    return objects


def get_s3_upload_input(s3_path: str) -> list:
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


### PP QC funtions
def condensed_to_square_index(i, j, n):
    """
    Convert (i, j) pair to condensed distance matrix index.
    Assumes i < j for condensed form.
    """
    if i == j:
        return None  # Diagonal not stored in condensed form
    if i > j:
        i, j = j, i  # Ensure i < j
    # Formula for condensed index
    return n * i - (i * (i + 1)) // 2 + j - i - 1


def get_distance(dists, i, j, n):
    """
    Get distance between samples i and j from condensed distance matrix.
    """
    if i == j:
        return 0.0
    idx = condensed_to_square_index(i, j, n)
    if idx is None or idx >= len(dists):
        return np.nan
    return dists[idx]


def analyze_core_distances(new_db, new_samples=None, core_threshold=0.1, accessory_threshold=0.1):
    """
    Analyze core and accessory distances for specific samples against references.
    
    Parameters:
    -----------
    new_db : str
        Path to the PopPUNK database directory
    new_samples : list
        List of sample names to analyze (if None, uses all query samples)
    core_threshold : float
        Threshold for marking core distance failures (default: 0.1)
    accessory_threshold : float
        Threshold for marking accessory distance failures (default: 0.1)
    """
    db_path = f"{new_db}/{new_db}"
    
    # Load names
    try:
        with open(f"{db_path}.dists.pkl", "rb") as f:
            names = pickle.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {db_path}.dists.pkl", file=sys.stderr)
        sys.exit(1)
    
    # Load distances - PopPUNK stores [core, accessory] as 2D array
    try:
        dists = np.load(f"{db_path}.dists.npy")
    except FileNotFoundError:
        print(f"Error: Could not find {db_path}.dists.npy", file=sys.stderr)
        sys.exit(1)
    
    ref_list = names[0]
    query_list = names[1]
    all_names = ref_list + query_list
    n_total = len(all_names)
    
    # If no specific samples provided, use all query samples
    if new_samples is None:
        new_samples = query_list
    
    print(f"Analyzing core and accessory distances for {len(new_samples)} samples against {len(ref_list)} references")
    print(f"Core threshold: {core_threshold}, Accessory threshold: {accessory_threshold}")
    print(f"Distance matrix shape: {dists.shape}")
    print()
    
    # Check if we have 2D distance matrix (core + accessory)
    has_accessory = len(dists.shape) > 1 and dists.shape[1] == 2
    
    # Print table header
    if has_accessory:
        print(f"{'Sample':<20} {'Min_Core':<10} {'Min_Acc':<10} {'Core_Fail':<12} {'Acc_Fail':<12} {'Status':<10}")
        print("-" * 84)
    else:
        print(f"{'Sample':<20} {'Min_Core':<10} {'Core_Fail':<12} {'Status':<10}")
        print("-" * 62)
        print("Note: Only core distances available in this database")
        print()
    
    results = []
    
    for sample in new_samples:
        try:
            idx = all_names.index(sample)
        except ValueError:
            print(f"Warning: Sample '{sample}' not found in database", file=sys.stderr)
            continue
        
        core_dists = []
        accessory_dists = []
        core_fail_count = 0
        acc_fail_count = 0
        
        for ref in ref_list:
            ref_idx = all_names.index(ref)
            i, j = min(idx, ref_idx), max(idx, ref_idx)
            condensed_idx = n_total * i - i * (i + 1) // 2 + j - i - 1
            
            if condensed_idx < len(dists):
                if has_accessory:
                    # Extract core (column 0) and accessory (column 1)
                    core_dist = dists[condensed_idx, 0]
                    acc_dist = dists[condensed_idx, 1]
                    
                    core_dists.append(core_dist)
                    accessory_dists.append(acc_dist)
                    
                    if core_dist > core_threshold:
                        core_fail_count += 1
                    if acc_dist > accessory_threshold:
                        acc_fail_count += 1
                else:
                    # Only core distances available
                    core_dist = dists[condensed_idx]
                    core_dists.append(core_dist)
                    
                    if core_dist > core_threshold:
                        core_fail_count += 1
        
        if core_dists:
            min_core = min(core_dists)
            core_fail = f"{core_fail_count} refs" if core_fail_count > 0 else "None"
            
            if has_accessory:
                min_acc = min(accessory_dists)
                acc_fail = f"{acc_fail_count} refs" if acc_fail_count > 0 else "None"
                
                # Overall pass/fail - fails if EITHER threshold is exceeded
                status = "FAIL" if (core_fail_count > 0 or acc_fail_count > 0) else "PASS"
                
                print(f'{sample:<20} {min_core:<10.4f} {min_acc:<10.4f} {core_fail:<12} {acc_fail:<12} {status:<10}')
                
                results.append({
                    'sample': sample,
                    'min_core': min_core,
                    'min_accessory': min_acc,
                    'mean_core': np.mean(core_dists),
                    'mean_accessory': np.mean(accessory_dists),
                    'max_core': np.max(core_dists),
                    'max_accessory': np.max(accessory_dists),
                    'core_fail_count': core_fail_count,
                    'acc_fail_count': acc_fail_count,
                    'total_refs': len(ref_list),
                    'status': status
                })
            else:
                status = "FAIL" if core_fail_count > 0 else "PASS"
                
                print(f'{sample:<20} {min_core:<10.4f} {core_fail:<12} {status:<10}')
                
                results.append({
                    'sample': sample,
                    'min_core': min_core,
                    'mean_core': np.mean(core_dists),
                    'max_core': np.max(core_dists),
                    'core_fail_count': core_fail_count,
                    'total_refs': len(ref_list),
                    'status': status
                })
        else:
            if has_accessory:
                print(f'{sample:<20} {"N/A":<10} {"N/A":<10} {"No data":<12} {"No data":<12} {"N/A":<10}')
            else:
                print(f'{sample:<20} {"N/A":<10} {"No data":<12} {"N/A":<10}')
    
    return results


def analyze_distances(new_db, distance_threshold=0.6):
    """
    Analyze PopPUNK distances and generate summary table.
    
    Parameters:
    -----------
    new_db : str
        Path to the PopPUNK database directory
    distance_threshold : float
        Threshold for marking failures (default: 0.6)
    """
    db_path = f"{new_db}/{new_db}"
    
    # Load names
    try:
        with open(f"{db_path}.dists.pkl", "rb") as f:
            names = pickle.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {db_path}.dists.pkl", file=sys.stderr)
        sys.exit(1)
    
    # Load distances
    try:
        dists = np.load(f"{db_path}.dists.npy")
    except FileNotFoundError:
        print(f"Error: Could not find {db_path}.dists.npy", file=sys.stderr)
        sys.exit(1)
    
    ref_list = names[0]
    query_list = names[1]
    
    n_ref = len(ref_list)
    n_query = len(query_list)
    n_total = n_ref + n_query
    
    print(f"Refs: {n_ref}, Queries: {n_query}, Total: {n_total}")
    print(f"Dists shape: {dists.shape}")
    print(f"Expected condensed pairs: {n_total * (n_total - 1) // 2}")
    print()
    
    # Combined order is ref_list + query_list
    all_names = ref_list + query_list
    
    # Print table header
    print(f"{'sample':<40} {'min_d':<10} {'mean_d':<10} {'max_d':<10} {'fail':<5} {'Fails(>0.6)':<15}")
    print("-" * 100)
    
    # Create results list for optional CSV output
    results = []
    
    # Analyze each query sample
    for q_idx, query_name in enumerate(query_list):
        # Query index in the combined list
        query_pos = n_ref + q_idx
        
        # Collect all distances for this query to all other samples
        query_distances = []
        fail_samples = []
        
        for sample_idx in range(n_total):
            if sample_idx == query_pos:
                continue  # Skip self-comparison
            
            dist = get_distance(dists, sample_idx, query_pos, n_total)
            
            if not np.isnan(dist):
                query_distances.append(dist)
                
                # Track failures (distances > threshold)
                if dist > distance_threshold:
                    fail_samples.append(all_names[sample_idx])
        
        # Calculate statistics
        if query_distances:
            min_d = np.min(query_distances)
            mean_d = np.mean(query_distances)
            max_d = np.max(query_distances)
            fail_count = len(fail_samples)
            fail_flag = "Y" if fail_count > 0 else "N"
        else:
            min_d = mean_d = max_d = np.nan
            fail_count = 0
            fail_flag = "N"
        
        # Print row
        fail_str = f"{fail_count} samples" if fail_count > 0 else "None"
        print(f"{query_name:<40} {min_d:<10.4f} {mean_d:<10.4f} {max_d:<10.4f} {fail_flag:<5} {fail_str:<15}")
        
        # Store results
        results.append({
            'sample': query_name,
            'min_d': min_d,
            'mean_d': mean_d,
            'max_d': max_d,
            'fail': fail_flag,
            'fail_count': fail_count,
            'fail_samples': ','.join(fail_samples) if fail_samples else ''
        })
    
    return results


def save_to_csv(results, output_file="poppunk_distance_summary.csv"):
    """
    Save results to CSV file.
    """
    import csv
    
    with open(output_file, 'w', newline='') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
    
    print(f"\nFull results saved to: {output_file}")


if __name__ == "__main__":

    # Get recent BB seqs list
    df = pd.read_csv(bb)
    seqs = df['sample'].apply(lambda x: x + "_T1").tolist()


    # Download command.run
    launch_bucket, path, base = get_s3_upload_input(run)

    s3_client.download_file(launch_bucket, path, base)
    cleaned = get_relevant_filepaths(base)

    # Download all relevant files in .command.run
    for pth in cleaned:
        launch_bucket, path, base = get_s3_upload_input(pth)
        s3_client.download_file(launch_bucket, path, base)
        if ".tar.gz" in base:
            process_db = base.replace("tar.gz","")


    # for troubleshooting, we remove --run-qc from poppunk assign from .command.sh
    with open(".command.sh", "r") as f:
        contents = f.read()

    contents = contents.replace("--run-qc", "")


    start = contents.find("--output")
    end = contents.find("--threads", start)

    if start != -1 and end != -1:
        new_db = contents[start + len("substring1")-1 : end].strip()

    with open(".command.sh", "w") as f:
        f.write(contents)


    # Run poppunk on docker image to make sure all packages same version as BigBacter
    docker_image = "public.ecr.aws/o8h2f0o1/poppunk:2.6.5"
    script_path = ".command.sh"
    cmd = [
        "docker", "run",
        "--rm",
        "-v", f"{os.getcwd()}:/work",
        "-w", "/work",
        docker_image,
        "bash", script_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    print("\n\n Beginning PP QC Check\n\n")


    ### QC Check
    # Check for core distance mode
    new_db = next(os.walk('.'))[1][0]
  
    # Run core distance analysis
    results = analyze_core_distances(
        new_db, 
        new_samples=seqs, 
        core_threshold=core,
        accessory_threshold=acc
    )
        

    save_to_csv(results, output_file=f"{output}.csv")
    