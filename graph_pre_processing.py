import pandas as pd
import numpy as np


def edge_list_pre_processing():
    with open('D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\CL-10M-1d8-L5.node_labels') as fin, open(
            'newfile.txt', 'w') as fout:
        for line in fin:
            fout.write(line.replace('\t', ','))
    for chunk in pd.read_csv("edges.csv", chunksize=10):
        print(chunk)


def probability_distribution():
    frame = pd.DataFrame()
    for chunk in pd.read_csv("edges.csv", chunksize=100000):
        print(chunk)
        chunk['probability'] = np.random.uniform(0, 1, chunk.shape[0])
        print(chunk)
        #print(chunk.head())
        frame = pd.concat([frame, chunk])
    frame.to_csv(r'D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\data\edge_list_with_probabilities.csv')
