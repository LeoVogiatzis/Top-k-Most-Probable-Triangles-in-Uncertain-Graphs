from pyspark.sql import SparkSession

formatter = 'com.databricks.spark.csv'
from graphframes import *

def driverPartitionAlgorithm(edges):
    p = 4

    def mapper(x):
        i = int(x[0]) % p
        j = int(x[1]) % p

        output = []

        for a in range(0, p):
            for b in range(a + 1, p):
                for c in range(b + 1, p):
                    if {i, j}.issubset({a, b, c}):
                        (output.append((str(a) + " " + str(b) + " " + str(c),
                                        [(x[0], x[1])])))
        return output

    mapper_output = (edges.rdd.flatMap(lambda x: mapper(x))
                     .reduceByKey(lambda x, y: x + y))

    def reducer(edge_list):
        no_triangles = 0

        def findTriangles(edges):
            import networkx as nx
            G = nx.Graph()
            for x in edges:
                G.add_edge(x[0], x[1])
            result = []
            done = set()  #
            for n in G:
                done.add(n)  #
                nbrdone = set()  #
                nbrs = set(G[n])
                for nbr in nbrs:
                    if nbr in done:  #
                        continue  #
                    nbrdone.add(nbr)  #
                    for both in nbrs.intersection(G[nbr]):
                        if both in done or both in nbrdone:  #
                            continue  #
                        result.append((n, nbr, both))
            return result

        triangles = findTriangles(edge_list)

        def weightedCount(x):
            u = int(x[0]) % p
            v = int(x[1]) % p
            w = int(x[2]) % p

            z = 1
            if u == v and v == w:
                z = (u * (u - 1) / 2) + u * (p - u - 1) + ((p - u - 1) * (p - u - 2) / 2)
            elif u == v or v == w or u == w:
                z = p - 2
            z = 1 / z
            # return (str(x[0])+" "+str(x[1])+" "+str(x[2]),z)
            return z

        for tri in triangles:
            no_triangles += weightedCount(tri)
        return ("*", no_triangles)

    reducer_output = mapper_output.map(lambda x: reducer(x[1]))
    return reducer_output.values().sum()


# spark = SparkSession \
#     .builder.master('local[1]') \
#     .appName("example").getOrCreate()

if __name__ == '__main__':
    # spark = SparkSession.builder.appName('graph').getOrCreate()
    # combined = spark.read.format(formatter).options(delimiter=' ', header='false', inferSchema=True) \
    #     .load('edgelist.txt').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
    #                                                                                                             'probs')
    # edges = combined.dropDuplicates(['src', 'dst'])
    #
    # vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()
    #
    # # create a dataframe with only one column
    # new_vertices = vdf.select(vdf['src'].alias('id')).distinct()
    # gf = GraphFrame(new_vertices, combined)
    # global edges, vectices
    spark = SparkSession \
        .builder.master('local[*]') \
        .appName("example-spark").getOrCreate()
    global edges, vectices
    edges = spark.createDataFrame([(1, 2), (1, 3), (2, 3), (3, 4), (3, 5), (4, 5)])
    vectices = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,), (6,)])
    #
    #
    # combined = spark.read.format(formatter).options(delimiter=',', header='false', inferSchema=True) \
    #     .load('edges.csv').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
    #                                                                                                          'probs')
    # edges = combined.dropDuplicates(['src', 'dst'])
    #
    # vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()
    #
    # # create a dataframe with only one column
    # new_vertices = vdf.select(vdf['src'].alias('id')).distinct()
    import time

    currentMilliTime = lambda: int(round(time.time() * 1000))

    t1 = currentMilliTime()
    # # Driver program to count the triangles
    driverPartitionAlgorithm(edges)
    # print("No. of Triangles:\t", output2)
    # print("No. of Triangles:\t", output1)
    t2 = currentMilliTime()
    time1 = t2 - t1
    print("NodeIterator Algorithm's Execution Time: \t\t {0} milliseconds.".format(time1))
    spark.stop()

