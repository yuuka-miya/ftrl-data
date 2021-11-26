import pandas as pd

import networkx as nx
import matplotlib.pyplot as plt

#df_fin1 = pd.read_csv("test_data.csv")

#line weight
lw = 5

graphs = {}

max_vertex = df_fin1['TOTAL_TRIPS'].max()

def graph_weight(data_row):
    global max_vertex
    
    if graphs(data_row['TIME_PER_HOUR']) is None:
        graphs(data_row['TIME_PER_HOUR']) = nx.DiGraph()
    graphs(data_row['TIME_PER_HOUR']).add_edge(data_row['ORIGIN_PT_CODE'], data_row['DESTINATION_PT_CODE'], (data_row['TOTAL_TRIPS']/max_vertex) * 5)
    
df_fin1[df_fin1['DAY_TYPE']].apply(graph_weight, axis=1)

for key, graph in graphs.values():
    pos = nx.kamada_kawai_layout(graph, scale=10)  # positions for all nodes

    # nodes
    nx.draw_networkx_nodes(graph, pos, node_size=5)

    # edges
    nx.draw_networkx_edges(graph, pos)

    # labels
    nx.draw_networkx_labels(graph, pos, font_size=5, font_family="sans-serif")

    plt.axis("off")
    plt.figure(1, figsize=(500,1000))
    plt.savefig("graphs/" + key + ".png", dpi=500)