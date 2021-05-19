import pandas as pd
from graphframes import *
from pyspark.sql import SparkSession
import numpy as np

# Import subpackage examples here explicitly so that this module can be
# run directly with spark-submit.


def edge_list_pre_processing():
    with open('D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\CL-10M-1d8-L5.node_labels') as fin, open(
            'newfile.txt', 'w') as fout:
        for line in fin:
            fout.write(line.replace('\t', ','))
    for chunk in pd.read_csv("edges.csv", chunksize=10):
        print(chunk)


def probability_distribution():
    frame = pd.DataFrame()
    for chunk in pd.read_csv("edges.csv", chunksize=10):
        print(chunk)
        chunk['probability'] = np.random.uniform(0, 1, chunk.shape[0])
        print(chunk.head())
        frame = pd.concat([frame, chunk])
    frame.to_csv(r'D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\data\edge_list_with_probabilities.csv')


def main():
    # edge_list_pre_processing()
    spark = SparkSession.builder.appName('graph').getOrCreate()
    edges = spark.read.csv('edges.csv', inferSchema=True, header=True)
    vertices = spark.read.csv('nodes.csv', inferSchema=True, header=True)
    # unionDF = edges.union(vertices)
    # display(unionDF)
    print(edges.show())
    print(vertices.show())
    # print(unionDF.show())
    g = GraphFrame(vertices, edges)
    ## Take a look at the DataFrames
    g.vertices.show()
    g.edges.show()
    ## Check the number of edges of each vertex
    g.degrees.show()


if __name__ == '__main__':
    probability_distribution()

# rels = pys.read.csv("D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\newfile.csv", header=True)
# reversed_rels = (rels.withColumn("newSrc", rels.dst)
# .withColumn("newDst", rels.src)
# .drop("dst", "src")
# .withColumnRenamed("newSrc", "src")
# .withColumnRenamed("newDst", "dst")
# .select("src", "dst", "relationship", "cost"))
# #

# #
# df = pd.read_csv('D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\newfile.csv')
# print(df.head(10))
# sc = SparkContext(master='local', appName='Spark Demo')
# print(sc.textFile('D:\Top-k-Most-Probable-Triangles-in-Uncertain-Graphs\newfile.txt').first())
