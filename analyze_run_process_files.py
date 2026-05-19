#!/usr/bin/env python3
"""
Analyze PopPUNK distances and populate summary table
"""

import pickle
import numpy as np
import sys
from pathlib import Path

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

def analyze_core_distances(new_db, new_samples=None, core_threshold=0.1, accessory_threshold=0.6):
    """
    Analyze core and accessory distances for specific samples against references.
    
    Parameters:
    -----------
    new_db : str
        Path to the PopPUNK database directory
    new_samples : list
        List of sample names to analyze (if None, uses all query samples)
    core_threshold : float
        Threshold for marking core distance failures (default: 0.1, matches --max-pi-dist)
    accessory_threshold : float
        Threshold for marking accessory distance failures (default: 0.6, matches --max-a-dist)
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
    
    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_poppunk_distances.py <poppunk_db_path> [OPTIONS]")
        print("\nModes:")
        print("  1. Full analysis (all samples, all distances):")
        print("     python analyze_poppunk_distances.py <db_path> [distance_threshold] [--csv]")
        print("\n  2. Core/Accessory distance analysis (samples vs references):")
        print("     python analyze_poppunk_distances.py <db_path> --core [samples] [core_thresh] [acc_thresh]")
        print("\nCore Mode Arguments:")
        print("  samples       : Comma-separated sample names (optional, defaults to all queries)")
        print("  core_thresh   : Core distance threshold (default: 0.1, matches --max-pi-dist)")
        print("  acc_thresh    : Accessory distance threshold (default: 0.6, matches --max-a-dist)")
        print("\nExamples:")
        print("  # Full analysis")
        print("  python analyze_poppunk_distances.py ./2026-bacteria 0.6 --csv")
        print("\n  # Core/accessory analysis - all query samples, default thresholds (0.1, 0.6)")
        print("  python analyze_poppunk_distances.py ./2026-bacteria --core")
        print("\n  # Specific samples with default thresholds")
        print("  python analyze_poppunk_distances.py ./2026-bacteria --core 2_T1,2_T2")
        print("\n  # Custom core threshold (0.15), default accessory (0.6)")
        print("  python analyze_poppunk_distances.py ./2026-bacteria --core 0.15")
        print("\n  # Custom both thresholds: core=0.15, accessory=0.8")
        print("  python analyze_poppunk_distances.py ./2026-bacteria --core 0.15 0.8")
        print("\n  # Specific samples with custom thresholds")
        print("  python analyze_poppunk_distances.py ./2026-bacteria --core 2_T1,2_T2 0.12 0.7 --csv")
        sys.exit(1)
    
    new_db = sys.argv[1]
    
    # Check for core distance mode
    if '--core' in sys.argv:
        core_idx = sys.argv.index('--core')
        
        # Get sample list if provided
        new_samples = None
        core_threshold = 0.1  # --max-pi-dist
        accessory_threshold = 0.6  # --max-a-dist
        
        args_after_core = sys.argv[core_idx + 1:]
        # Filter out --csv flag
        args_after_core = [arg for arg in args_after_core if arg != '--csv']
        
        if len(args_after_core) >= 1:
            # First arg could be sample list or core threshold
            try:
                core_threshold = float(args_after_core[0])
                # If it's a number, check for accessory threshold
                if len(args_after_core) >= 2:
                    try:
                        accessory_threshold = float(args_after_core[1])
                    except ValueError:
                        pass
            except ValueError:
                # It's a sample list
                new_samples = args_after_core[0].split(',')
                
                # Check for thresholds after sample list
                if len(args_after_core) >= 2:
                    try:
                        core_threshold = float(args_after_core[1])
                        # Check for accessory threshold
                        if len(args_after_core) >= 3:
                            try:
                                accessory_threshold = float(args_after_core[2])
                            except ValueError:
                                pass
                    except ValueError:
                        pass
        
        # Run core distance analysis
        results = analyze_core_distances(
            new_db, 
            new_samples=new_samples, 
            core_threshold=core_threshold,
            accessory_threshold=accessory_threshold
        )
        
        # Save to CSV if requested
        if '--csv' in sys.argv:
            save_to_csv(results, output_file="poppunk_core_distances.csv")
    
    else:
        # Full analysis mode
        threshold = float(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != '--csv' else 0.6
        save_csv = '--csv' in sys.argv
        
        # Run full analysis
        results = analyze_distances(new_db, distance_threshold=threshold)
        
        # Optionally save to CSV
        if save_csv:
            save_to_csv(results)
