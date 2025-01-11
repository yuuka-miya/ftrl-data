from collections import deque, namedtuple
import pandas as pd
import json
from tqdm import tqdm
import itertools
import numpy as np
import networkx as nx

#cross platform interchanges - they share the same node
interchange_codes = {
    "EW24": "NS1",
    "EW13": "NS25",
    "EW14": "NS26",
    "DT16": "CE1"
    }

interchange_nodes = []

# we'll use infinity as a default distance to nodes.
inf = float('inf')
Edge = namedtuple('Edge', 'start, end, cost')

results = {}

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return list(a), list(b)  
  
print("Running pathfinder AND route walker!")
        
graph = nx.Graph()
df = pd.read_csv("nodes.csv")
df = df.replace({'n1': interchange_codes, 'n2': interchange_codes})
df1 = pd.read_csv("stations.csv")
df1 = df1.replace({'stn_code': interchange_codes})
df1 = df1.drop_duplicates()
df2 = pd.DataFrame()
for index, row in df.iterrows():
    graph.add_edge(row['n1'], row['n2'], weight=row['time'])

# interchange station handling - weight 99 to the joint code so that only entries/exits use it
for index, row in df1.iterrows():
  code = row["stn_code"]
  break_code = code.split('/')
  if len(break_code) > 1:
    interchange_nodes.append(code)
    for subcode in break_code:
      graph.add_edge(code, subcode, weight=99)
    
for index1, row1 in tqdm(df1['stn_code'].items(), total = df1.size):
#row1 = "BP10"
  list1 = []
  for index2, row2 in df1['stn_code'].items():
    if row2 != row1:
      if row1 not in results:
        results[row1] = {}
      if row2 not in results[row1]:
        results[row1][row2] = []

      results[row1][row2] = list(nx.dijkstra_path(graph, row1, row2))
  
with open ("train_routes_nx.json", "w") as outfile:
    json.dump(results, outfile, sort_keys=True, indent=4, ensure_ascii=False)

# remove our hack nodes
for u in interchange_nodes:
   graph.remove_node(u)
    
import matplotlib.pyplot as plt
pos = nx.kamada_kawai_layout(graph, scale=10)  # positions for all nodes

# nodes
nx.draw_networkx_nodes(graph, pos, node_size=5)
print(1)

# edges
nx.draw_networkx_edges(graph, pos)

# labels
nx.draw_networkx_labels(graph, pos, font_size=5, font_family="sans-serif")

plt.axis("off")
plt.figure(1, figsize=(500,1000))
plt.savefig("graph_tel3.png", dpi=500)