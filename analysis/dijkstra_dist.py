from collections import deque, namedtuple
import pandas as pd
import json
from tqdm import tqdm
import itertools
import numpy as np
import networkx as nx

results = {}
nets_used = {}

interchange_codes = {
    "EW24": "NS1",
    "EW13": "NS25",
    "EW14": "NS26",
    "DT15": "CC4",
    "CC1": "NS24",
    "NE6": "NS24",
    "BP1": "NS4",
    "CC15": "NS17",
    "CE2": "NS27",
    "CC9": "EW8",
    "DT14": "EW12",
    "NE3": "EW16",
    "CC22": "EW21",
    "DT35": "CG1",
    "CC29": "NE1",
    "DT19": "NE4",
    "DT12": "NE7",
    "CC13": "NE12",
    "STC": "NE16",
    "PTC": "NE17",
    "DT26": "CC10",
    "DT9": "CC19",
    "DT16": "CE1",
    "TE2": "NS9",
    "TE9": "CC17",
    "TE11": "DT10",
    "TE14": "NS22",
    "TE17": "EW16",
    "TE20": "NS27",
    "TE31": "DT37",
    "FL1": "CC32",
    "JS1": "NS4",
    "JS8": "EW27",
    "JE5": "NS1",
    "CR5": "EW1",
    "CR8": "NE14",
    "CR11": "NS16",
    "CR13": "TE7",
	  "CP4": "NE17",
    "CR15": "DT6",
    "CR17": "EW23",
    #as leaked in online exhibit
    "CR21": "JS12",
    "CR24": "EW30",
    #unpaid links
    "BP6": "DT1",
    "DT32": "EW2",
    "DT11": "NS21",
    "CP3": "PE4"
    }

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return list(a), list(b)  
  
print("Running pathfinder AND route walker!")
        
graph = nx.Graph()
df = pd.read_csv("nodes_dist_net.csv")
df = df.replace({'n1': interchange_codes, 'n2': interchange_codes})
df1 = pd.read_csv("stations.csv")
df1 = df1.replace({'stn_code': interchange_codes})
df1 = df1.drop_duplicates()
df2 = pd.DataFrame()
for index, row in df.iterrows():
    graph.add_edge(row['n1'], row['n2'], weight=row['time'], net=row["network"])
    
for index1, row1 in tqdm(df1['stn_code'].iteritems(), total = df1.size):
#row1 = "BP10"
  list1 = []
  for index2, row2 in df1['stn_code'].iteritems():
    if row2 != row1:
      if row1 not in results:
        results[row1] = {}
      if row2 not in results[row1]:
        results[row1][row2] = []
      if row1 not in nets_used:
        nets_used[row1] = {}
      if row2 not in nets_used[row1]:
        nets_used[row1][row2] = []
      results[row1][row2] = list(nx.dijkstra_path(graph, row1, row2))
      nets = [graph.edges[ea[0], ea[1]]["net"] for ea in nx.path_graph(results[row1][row2] ).edges()]
      nets_used[row1][row2] = list(dict.fromkeys(nets).keys())
  
with open ("train_routes_nets_tel3_dist.json", "w") as outfile:
    json.dump(results, outfile, sort_keys=True, indent=4, ensure_ascii=False)

with open ("nets_used_nx_tel3_dist.json", "w") as outfile:
    json.dump(nets_used, outfile, sort_keys=True, indent=4, ensure_ascii=False)

import copy
new_graph = copy.deepcopy(graph)
for edge in graph.edges(data=True):
    if edge[2]["net"] == "INT":
        print(edge)
        new_graph = nx.contracted_edge(new_graph, (edge[0], edge[1]))
        new_graph.remove_edge(edge[0], edge[0])
       
    
import matplotlib.pyplot as plt
pos = nx.kamada_kawai_layout(new_graph, scale=10)  # positions for all nodes

# nodes
nx.draw_networkx_nodes(new_graph, pos, node_size=5)

# edges
nx.draw_networkx_edges(new_graph, pos)

# labels
nx.draw_networkx_labels(new_graph, pos, font_size=5, font_family="sans-serif")

plt.axis("off")
plt.figure(1, figsize=(500,1000))
plt.savefig("graph_tel3_dist_ungraphed.png", dpi=500)