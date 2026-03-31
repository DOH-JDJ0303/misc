import json
import argparse
from argparse import ArgumentParser
import pandas as pd
from io import StringIO
import boto3
import os
import plotly.graph_objects as go

"""
This script compares clustering/partitioning in BigBacter v1 to v2 nad outputs 
a sankey plot to visualize the cluster/partitioning comparison. Any samples 
that were removed from a cluster set in v1 and place in another in v2 will be 
be listed in the terminal and the clusters that they are a part of will be 
highlighted in red in the sankeyplot

Example Usage:
python bigbacter_comp.py --new_cluster_files \
    s3://<bucket>/bigbacter/v2.0/results/<bbv2timestamp>/Klebsiella_pneumoniae/ \
    --new_db s3://<bucket>/bigbacter/v2.0/db/Klebsiella_pneumoniae/<bbv2timestamp>/ \
    --old_db s3://<bucket>/bigbacter/db/<bbv1timestamp>/ --old_cluster_files \
    s3://<bucket>/bigbacter/v1.0/<bbv1timestamp>/Klebsiella_pneumoniae/ \
    --output kleb_pneumo_sankey.png 
"""

parser = ArgumentParser()
parser.add_argument("--microreact", dest="mr", help='microreact file')
parser.add_argument("--old_cluster_files", dest="old", help='dir of bb_v1 run')
parser.add_argument("--new_cluster_files", dest="new", help='dir of bb_v2 run. May be in .../<your_outdir>/<timestamp>/')
parser.add_argument("--new_db", dest="new_db", default=None, help='db path used as param in bb_v2 run')
parser.add_argument("--old_db", dest="old_db", default=None, help='db path used as param in bb_v1 run')
parser.add_argument("--output", dest="out", default=None, help='<output>.png for sankey plot')
parser.add_argument('--ignore_recombination', action=argparse.BooleanOptionalAction)


args = parser.parse_args() 
old = args.old
new = args.new
new_db = args.new_db
old_db = args.old_db
out = args.out
ig = args.ignore_recombination

s3_client = boto3.client('s3')


def sankey_plot(df, outfile):
    df['old_cluster_partition'] = df['old_cluster'].astype(str) + "_" + df['old_partition'].astype(str)
    df['new_cluster_partition'] = df['new_cluster'].astype(str) + "_" + df['new_partition'].astype(str)

    flows = df.groupby(["old_cluster_partition", "new_cluster_partition"]).size().reset_index(name="count")

    # Labels
    labels_old = [f"old_{c}" for c in sorted(df.old_cluster_partition.unique())]
    labels_new = [f"new_{c}" for c in sorted(df.new_cluster_partition.unique())]
    labels = labels_old + labels_new

    # Node index maps
    map_old = {c: i for i, c in enumerate(sorted(df.old_cluster_partition.unique()))}
    map_new = {c: i + len(labels_old) for i, c in enumerate(sorted(df.new_cluster_partition.unique()))}

    sources = flows["old_cluster_partition"].map(map_old)
    targets = flows["new_cluster_partition"].map(map_new)
    values = flows["count"]

    # ---------------------------------------------------------
    # 🔥 Dynamic sizing based on number of nodes
    # ---------------------------------------------------------
    num_old = df['old_cluster_partition'].nunique()
    num_new = df['new_cluster_partition'].nunique()
    num_nodes = num_old + num_new

    width = max(800, num_nodes * 40)
    height = max(600, num_nodes * 30)

    thickness = min(40, max(10, num_nodes // 2))
    pad = min(40, max(10, num_nodes // 3))

    font_size = max(10, min(20, 200 // max(1, num_nodes)))
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # 🔥 Identify violating samples (your logic unchanged)
    # ---------------------------------------------------------
    split_old_parts = (
        df.groupby('old_cluster_partition')['new_cluster_partition']
        .nunique()
        .loc[lambda s: s > 1]
        .index
    )

    violating_samples = []

    for old_part in split_old_parts:
        new_parts = df.loc[df['old_cluster_partition'] == old_part, 'new_cluster_partition'].unique()

        for new_part in new_parts:
            contributing_old_parts = df.loc[
                df['new_cluster_partition'] == new_part, 'old_cluster_partition'
            ].unique()

            if len(contributing_old_parts) > 1:
                bad_samples = df.loc[
                    (df['old_cluster_partition'] == old_part) &
                    (df['new_cluster_partition'] == new_part),
                    'ID'
                ].tolist()

                violating_samples.extend(bad_samples)

    print("Violating samples:", violating_samples)

    # ---------------------------------------------------------
    # 🔥 Determine which partitions contain violating samples
    # ---------------------------------------------------------
    violating_old_parts = df.loc[df["ID"].isin(violating_samples), "old_cluster_partition"].unique()
    violating_new_parts = df.loc[df["ID"].isin(violating_samples), "new_cluster_partition"].unique()

    # Node colors
    node_colors = []
    for c in sorted(df.old_cluster_partition.unique()):
        node_colors.append("red" if c in violating_old_parts else "lightblue")

    for c in sorted(df.new_cluster_partition.unique()):
        node_colors.append("red" if c in violating_new_parts else "lightblue")

    # Link colors
    link_colors = []
    for _, row in flows.iterrows():
        old_p = row["old_cluster_partition"]
        new_p = row["new_cluster_partition"]

        if (old_p in violating_old_parts) or (new_p in violating_new_parts):
            link_colors.append("rgba(255,0,0,0.6)")  # semi-transparent red
        else:
            link_colors.append("rgba(150,150,255,0.4)")  # soft blue
    # ---------------------------------------------------------

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=pad,
                    thickness=thickness,
                    label=labels,
                    color=node_colors,
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=link_colors,
                )
            )
        ]
    )

    fig.update_layout(
        title_text="Cluster Comparison Sankey Diagram",
        font_size=font_size,
        width=width,
        height=height
    )

    fig.write_image(outfile)






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


def pick_microreact_files(s3_objs, ig=ig):
    temp_dict = {}
    for url in s3_objs:
        #print(url)    
        cluster = url.split("/")[-3]
        if cluster not in temp_dict.keys():
            temp_dict[cluster] = [url]
        else:
            # print(old_dict[cluster], "blahblah", url, "raw")
            temp_dict[cluster].append(url)

    temp_mr = []
    temp_singlets = []

    for key in temp_dict.keys():
        if temp_dict[key] is None:
            temp_singlets.append(key)
        elif len(temp_dict[key]) == 1:
            temp_mr.append(temp_dict[key][0])
        else:
            if not ig: # ignoring recombinant-masked results
                if any("ubbins" in item for item in temp_dict[key]):
                    temp_mr.append(list(filter(lambda x: "ubbins" in x, temp_dict[key]))[0])
                else:
                    temp_mr.append(list(filter(lambda x: "masked" in x, temp_dict[key]))[0])
            else:
                if any("ubbins" in item for item in temp_dict[key]):
                    temp_mr.append(list(filter(lambda x: "ubbins" not in x, temp_dict[key]))[0])
                else:
                    temp_mr.append(list(filter(lambda x: "masked" not in x, temp_dict[key]))[0])
    

    return temp_mr, temp_singlets


def s3_download_microreacts(common, path_list):
    file_paths = []

    for obj in path_list:
        if "s3://" in common:
            bucket = common.replace("s3://", "").split("/")[0]
            local = obj.split("/")[-1]
            s3_client.download_file(bucket, obj, local)
            file_paths.append(local)
    return file_paths

def get_clusters_from_microreact(microreact_file):
    with open(microreact_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
        if 'metadata' not in data['files'].keys():
            csv_buffer = StringIO(data['files']['summary_file']['blob'])
            df = pd.read_csv(csv_buffer)
            df.columns.values[0] = 'ID'
            df['ID'] = df['ID'].str.replace('_T1', '', regex=False)
            df = df.rename(columns={"cluster": "new_cluster"})
            if "partition" not in df.columns:
                df["new_partition"] = 1
            else:
                df = df.rename(columns={"partition": "new_partition"})

            df = df[["ID","new_cluster","new_partition"]]
            result_dict = dict(zip(df["ID"], df[["new_cluster", "new_partition"]].values.tolist()))

        else:
            csv_buffer = StringIO(data['files']['metadata']['blob'])
            df = pd.read_csv(csv_buffer)
            df['ID'] = df['ID'].str.replace('_T1', '', regex=False)
            if "PARTITION" not in df.columns:
                df["old_partition"] = 1
            else:
                df = df.rename(columns={"PARTITION": "old_partition"})
            df = df.rename(columns={"CLUSTER": "old_cluster"})
            df = df[["ID","old_cluster","old_partition"]]
            result_dict = dict(zip(df["ID"], df[["old_cluster", "old_partition"]].values.tolist()))

        return result_dict


def get_all_clusters(microreact_files):
    merged = {}
    for fi in microreact_files:
        result_dict = get_clusters_from_microreact(fi)
        merged.update(result_dict)
    return merged

def remove_downloaded(files):
    for fi in files:
        os.remove(fi.split("/")[0])


### Get new version clusters ###
new_urls = list_s3_objects(new)
new_urls = [item for item in new_urls if ".microreact" in item and "report" in item]

new_mr, new_singlets = pick_microreact_files(new_urls)
print(new_singlets)


local_new_mr = s3_download_microreacts(new, new_mr)
new_clusters = get_all_clusters(local_new_mr)
new_clusters = {k: v for k, v in new_clusters.items() if "eference" not in k}

df = pd.DataFrame.from_dict(new_clusters, orient="index", columns=["new_cluster", "new_partition"])
df = df.reset_index().rename(columns={"index": "ID"})

new_df = df.copy()

print('new results gathered, onto old...')
### Get old version clusters ###
old_urls = list_s3_objects(old)
old_urls = [item for item in old_urls if ".microreact" in item and "figure" in item]
old_mr, old_singlets = pick_microreact_files(old_urls)



local_old_mr = s3_download_microreacts(old, old_mr)
old_clusters = get_all_clusters(local_old_mr)
df = pd.DataFrame.from_dict(old_clusters, orient="index", columns=["old_cluster", "old_partition"])
df = df.reset_index().rename(columns={"index": "ID"})

print("old results gathered, time to clean up!")
### Cleanup ###


print("cleanup done! Now to compare...")
df = df.merge(new_df, on="ID", how="inner")
# Convert cluster/partition columns to int
cols = ['old_cluster', 'old_partition', 'new_cluster', 'new_partition']
print(df[df.isna().any(axis=1)])
df = df.dropna(subset=["new_cluster", "new_partition","old_cluster", "old_partition"])
df[cols] = df[cols].astype(int)
print(df)

sankey_plot(df, out)

remove_downloaded(local_old_mr)
remove_downloaded(local_new_mr)

