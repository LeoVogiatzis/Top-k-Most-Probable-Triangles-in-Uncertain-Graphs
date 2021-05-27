from graphframes import *
from pyspark.sql import SparkSession

# Import subpackage examples here explicitly so that this module can be
# run directly with spark-submit.
formatter = 'com.databricks.spark.csv'


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


# triangles = findTriangles(edge_list)


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
    triangles = findTriangles(edges)
    x = 1
    g.vertices.show()
    g.edges.show()
    ## Check the number of edges of each vertex
    g.degrees.show()


def get_metrics(gf):
    gf.vertices.show()
    gf.edges.show()
    ## Check the number of edges of each vertex
    gf.degrees.show()
    result = gf.triangleCount()
    (result.sort("count", ascending=False)
     .filter('count > 0')
     .show())


def create_graph():
    spark = SparkSession.builder.appName('graph').getOrCreate()
    combined = spark.read.format(formatter).options(delimiter=' ', header='false', inferSchema=True) \
        .load('edgelist.txt').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
                                                                                                                'probs')
    combined = combined.dropDuplicates(['src', 'dst'])

    vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()

    # create a dataframe with only one column
    new_vertices = vdf.select(vdf['src'].alias('id')).distinct()

    # new_edges = combined.join(new_vertices, combined['src'] == new_vertices['id'], 'left')
    # new_edges = new_edges.select(new_edges['dst'], new_edges['id'].alias('src'))
    #
    # new_edges = new_edges.join(new_vertices, new_edges['dst'] == new_vertices['id'], 'left')
    # new_edges = new_edges.select(new_edges['src'], new_edges['id'].alias('dst'))

    # created graph only with connections among vertices
    gf = GraphFrame(new_vertices, combined)
    print(gf.cache())

    get_metrics(gf)
    return gf


if __name__ == '__main__':
    gf = create_graph()
    # edge_list_pre_processing()
    # probability_distribution()
    # main()

    # probability_distribution()

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
