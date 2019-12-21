from collections import deque, namedtuple
import pandas as pd
import json
from tqdm import tqdm
import itertools
import numpy as np

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
    "JE5": "EW24-NS1"
    
    }

# we'll use infinity as a default distance to nodes.
inf = float('inf')
Edge = namedtuple('Edge', 'start, end, cost')

results = {}

def make_edge(start, end, cost=1):
  return Edge(start, end, cost)


class Graph:
    def __init__(self, edges):
        # let's check that the data is right
        wrong_edges = [i for i in edges if len(i) not in [2, 3]]
        if wrong_edges:
            raise ValueError('Wrong edges data: {}'.format(wrong_edges))

        self.edges = [make_edge(*edge) for edge in edges]
    def __init__(self):
        self.edges = []

    @property
    def vertices(self):
        return set(
            sum(
                ([edge.start, edge.end] for edge in self.edges), []
            )
        )

    def get_node_pairs(self, n1, n2, both_ends=True):
        if both_ends:
            node_pairs = [[n1, n2], [n2, n1]]
        else:
            node_pairs = [[n1, n2]]
        return node_pairs

    def remove_edge(self, n1, n2, both_ends=True):
        node_pairs = self.get_node_pairs(n1, n2, both_ends)
        edges = self.edges[:]
        for edge in edges:
            if [edge.start, edge.end] in node_pairs:
                self.edges.remove(edge)

    def add_edge(self, n1, n2, cost=1, both_ends=True):
        node_pairs = self.get_node_pairs(n1, n2, both_ends)
        for edge in self.edges:
            if [edge.start, edge.end] in node_pairs:
                return ValueError('Edge {} {} already exists'.format(n1, n2))

        self.edges.append(Edge(start=n1, end=n2, cost=cost))
        if both_ends:
            self.edges.append(Edge(start=n2, end=n1, cost=cost))

    @property
    def neighbours(self):
        neighbours = {vertex: set() for vertex in self.vertices}
        for edge in self.edges:
            neighbours[edge.start].add((edge.end, edge.cost))

        return neighbours

    def dijkstra(self, source, dest):
        assert source in self.vertices, 'Such source node doesn\'t exist'
        distances = {vertex: inf for vertex in self.vertices}
        previous_vertices = {
            vertex: None for vertex in self.vertices
        }
        distances[source] = 0
        vertices = self.vertices.copy()

        while vertices:
            current_vertex = min(
                vertices, key=lambda vertex: distances[vertex])
            vertices.remove(current_vertex)
            if distances[current_vertex] == inf:
                break
            for neighbour, cost in self.neighbours[current_vertex]:
                alternative_route = distances[current_vertex] + cost
                if alternative_route < distances[neighbour]:
                    distances[neighbour] = alternative_route
                    previous_vertices[neighbour] = current_vertex

        path, current_vertex = deque(), dest
        while previous_vertices[current_vertex] is not None:
            path.appendleft(current_vertex)
            current_vertex = previous_vertices[current_vertex]
        if path:
            path.appendleft(current_vertex)
        return path

def pairwise(iterable):
  "s -> (s0,s1), (s1,s2), (s2, s3), ..."
  a, b = itertools.tee(iterable)
  next(b, None)
  return zip(a, b)
  
print("Running pathfinder AND route walker!")
        
graph = Graph()
df = pd.read_csv("nodes.csv")
df = df.replace({'n1': interchange_codes, 'n2': interchange_codes})
df1 = pd.read_csv("stations.csv")
df1 = df1.replace({'stn_code': interchange_codes})
df1 = df1.drop_duplicates()
df2 = pd.DataFrame()
df_src = pd.read_csv("od201911.csv")
for index, row in df.iterrows():
    graph.add_edge(row['n1'], row['n2'], row['time'])
    
for index1, row1 in tqdm(df1['stn_code'].iteritems(), total = df1.size):
#row1 = "BP10"
  list1 = []
  for index2, row2 in df1['stn_code'].iteritems():
    if row2 != row1:
      if row1 not in results:
        results[row1] = {};
      if row2 not in results[row1]:
        results[row1][row2] = []
      results[row1][row2] = list(graph.dijkstra(row1, row2))
      for pair in pairwise(results[row1][row2]):
        data1 = df_src.loc[(df_src["ORIGIN_PT_CODE"] == row1) & (df_src["DESTINATION_PT_CODE"] == row2)]
        
        if (data1[data1["DAY_TYPE"] == "WEEKDAY"].empty !=  True):
          list1.append({"daytype": "WEEKDAY", "origin": pair[0], "dest": pair[1], "count": data1[data1["DAY_TYPE"] == "WEEKDAY"]["TOTAL_TRIPS"].values[0]})
        if (data1[data1["DAY_TYPE"] == "WEEKENDS/HOLIDAY"].empty != True):
          list1.append({"daytype": "WEEKENDS/HOLIDAY", "origin": pair[0], "dest": pair[1], "count": data1[data1["DAY_TYPE"] == "WEEKENDS/HOLIDAY"]["TOTAL_TRIPS"].values[0]})

  df2 = df2.append(list1)
df2 = df2.groupby(['daytype', 'origin', 'dest']).sum()
df2.to_csv("walked_routes.csv")
  
with open ("train_routes.json", "w") as outfile:
    json.dump(results, outfile, sort_keys=True, indent=4, ensure_ascii=False)