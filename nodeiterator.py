from pyspark import SparkConf
from pyspark.sql import SparkSession

formatter = 'com.databricks.spark.csv'


def spark_init():
    sparkConf = SparkConf().setMaster("local[2]")
    spark = SparkSession \
        .builder \
        .appName("NodeIterator") \
        .config(conf=sparkConf) \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def create_graph():
    combined = spark.read.format(formatter).options(delimiter=' ', header='false', inferSchema=True) \
        .load('edgelist.txt').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
                                                                                                             'probs')
    edges = combined.dropDuplicates(['src', 'dst'])

    vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()

    # create a dataframe with only one column
    new_vertices = vdf.select(vdf['src'].alias('id')).distinct()
    edges.show()
    return edges, new_vertices


def find_triangles(edges):
    """

    :param edges: edgelist
    :return: map reduce node_iterator we get the list and return all triangles
    """
    edges = edges.rdd. \
        map(lambda x: (x[0], x[1]) if (x[0] < x[1]) else (x[1], x[0]))  # .sortBy(lambda x: -x[2])
    edges.foreach(print)
    print('-------------')
    output_map1 = edges.map(lambda x: (x[0], [x[1]])
    if x[0] < x[1]
    else (x[1], [x[0]])) \
        .filter(lambda x: x != None) \
        .reduceByKey(lambda x, y: x + y)
    output_map1.foreach(print)

    def reducer1(x):
        output = []
        for a in range(0, len(x[1])):
            for b in range(a + 1, len(x[1])):
                output.append(((x[1][a], x[1][b]), [x[0]]))
        return output

    output_reducer1 = output_map1.flatMap(reducer1)
    print('--------------')
    output_reducer1.foreach(print)
    output_reducer2 = edges.map(lambda x: ((x[0], x[1]), ["*"]))
    print('--------------')
    output_reducer2.foreach(print)
    output_reducer2 = output_reducer2.union(output_reducer1)
    print('--------------')
    output_reducer2.foreach(print)
    output = output_reducer2.reduceByKey(lambda x, y: x + y).filter(lambda y: len(y) > 1).collect()
    print('--------------')
    print(output)

    def generate_triplets(x):
        output = []
        for tupples in x:
            vertex_list = tupples[1]
            if "*" in vertex_list and len(vertex_list) != 1:
                vertex_list = set(vertex_list) - {"*"}
                for vertex in vertex_list:
                    output.append((tupples[0][0], tupples[0][1], vertex))

        yield output
        print(output)

    # print(f'triangles:{generateTriplets(output)}')
    return list(generate_triplets(output))


def driver_node_iterator(edges):
    """
    Driver
    :return: yield the asked results
    """
    return find_triangles(edges)


def get_edges(spark):
    """

    :param spark: Read edge_list using DataFrame API
    :return: edges, vertices
    """
    edges = spark.read.format(formatter).options(delimiter=' ', header='false', inferSchema=True) \
        .load('rdy_geo100k.csv').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed(
        '_c2',
        'probs')
    # partition_edges = edges2.partitionBy(4)
    vertices = (edges.select(edges['src']).union(edges.select(edges['dst']))).distinct()
    return edges, vertices


if __name__ == '__main__':
    spark, sc = spark_init()
    edges, vectices = get_edges(spark)

    import time

    currentMilliTime = lambda: int(round(time.time() * 1000))

    t1 = currentMilliTime()
    output1 = driver_node_iterator(edges)
    # print("No. of Triangles:\t", output1)
    t2 = currentMilliTime()
    time1 = t2 - t1
    print("NodeIterator Algorithm's Execution Time: \t\t {0} milliseconds.".format(time1))
    spark.stop()
