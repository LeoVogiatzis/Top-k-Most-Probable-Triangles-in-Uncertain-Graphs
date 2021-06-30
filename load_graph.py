from graphframes import *

from pyspark.sql import SparkSession

# Import subpackage examples here explicitly so that this module can be
# run directly with spark-submit.
formatter = 'com.databricks.spark.csv'


def get_metrics(gf):
    gf.vertices.show()
    gf.edges.show()
    ## Check the number of edges of each vertex
    gf.degrees.show()
    result = gf.triangleCount()
    (result.sort("count", ascending=False)
     .filter('count > 0').count())
    print(result)
    # data = gf.triplets.toPandas()
    # # get_triangles(gf)
    # print(data.hist)


def create_graph():
    spark = SparkSession.builder.appName('graph').getOrCreate()
    combined = spark.read.format(formatter).options(delimiter=' ', header='false', inferSchema=True) \
        .load('edgelist.txt').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
                                                                                                                'probs')
    combined = combined.dropDuplicates(['src', 'dst'])

    vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()

    # create a dataframe with only one column
    new_vertices = vdf.select(vdf['src'].alias('id')).distinct()
    print(new_vertices.show())
    print(combined.show())

    # created graph only with connections among vertices
    gf = GraphFrame(new_vertices, combined)
    print(gf.cache())

    get_metrics(gf)

    return gf, new_vertices, combined


if __name__ == '__main__':
    gf, new_vertices, edges = create_graph()

