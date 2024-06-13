import sys
import pandas as pd
import subprocess
from argparse import ArgumentParser
from typing import List


parser = ArgumentParser()
parser.add_argument('--db', required=True, help='path/to/db')
parser.add_argument("--min_clusters", default="2", type=int, help="minimum clusters to test")
parser.add_argument("--max_clusters", default="3", type=int, help="maximum clusters to test")
parser.add_argument("--step_size", dest="steps",  default="1", type=int, \
                    help="step increment to best between cluster min and max")
parser.add_argument('--analysis-args', dest='pp_args', default="", 
                    help="Other arguments to pass to poppunk. e.g. ""'--thread 8'")

args = parser.parse_args()
db = args.db
min_clusters = args.min_clusters
max_clusters = args.max_clusters
steps = args.steps
pp_args = args.pp_args

def fit_test(cluster_num:str, db:str, pp_args:str, add_list:List):
    sys.stderr.write("Running --fit-model bgmm\n")
    bgmm_cmd = \
        "poppunk --fit-model bgmm  --ref-db " + db + \
        " --output " + db + "--overwrite " + \
        "--K " + str(cluster_num) + ""+ pp_args
    sys.stderr.write(bgmm_cmd + "\n")

    try:
        proc = subprocess.run(bgmm_cmd, shell=True, check=True, \
                            universal_newlines = True, \
                            encoding='utf-8', capture_output=True)

        row_list = [str(cluster_num)]
        for i in str(proc.stderr).split("\n"):
            if "Score" in i or "Avg. entropy of assignment" in i:
                #print(i)
                row_list.append(i.split("\t")[-1])
    except:
        row_list = []
        print(f"{cluster_num} distinct component(s) could not be found.")

    return row_list


if __name__ == "__main__":
    cluster_scores=[]
    for i in range(min_clusters, max_clusters+1, steps):
        cluster_scores.append(fit_test(i, db, pp_args, cluster_scores))


    cluster_scores = [ls for ls in cluster_scores if ls != []]
    df = pd.DataFrame(cluster_scores, columns = ['K', 'avg entropy', "Score", "Score (w/ betweenness)", "Score (w/ weighted-betweenness)"]) 
    df.to_csv(db+"_min-clust"+str(min_clusters)+"_max-clust"+str(max_clusters)+'.csv', index=False)
    print("see results in", db+"_min-clust"+str(min_clusters)+"_max-clust"+str(max_clusters)+'.csv')
