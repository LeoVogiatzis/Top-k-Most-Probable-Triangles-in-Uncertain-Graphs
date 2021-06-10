from pyspark.sql import SparkSession

formatter = 'com.databricks.spark.csv'


def findTriangles(edges):
    edges = edges.rdd. \
        map(lambda x: (x[0], x[1], x[2]) if (x[0] < x[1]) else (x[1], x[0], x[2])).sortBy(lambda x: -x[2], ascending=False)

    output_map1 = edges.map(lambda x: (x[0], [x[1]])
    if x[0] < x[1]
    else (x[1], [x[0]])) \
        .filter(lambda x: x != None) \
        .reduceByKey(lambda x, y: x + y)
    # .foreach(print)

    def reducer1(x):
        output = []
        for a in range(0, len(x[1])):
            for b in range(a + 1, len(x[1])):
                output.append(((x[1][a], x[1][b]), [x[0]]))
        return output

    output_reducer1 = output_map1.flatMap(reducer1)
    output_reducer2 = edges.map(lambda x: ((x[0], x[1]), ["*"]))
    output_reducer2 = output_reducer2.union(output_reducer1)
    output = output_reducer2.reduceByKey(lambda x, y: x + y).filter(lambda y: len(y) > 1).collect()

    def generateTriplets(x):
        output = []
        for tupples in x:
            vertex_list = tupples[1]
            if "*" in vertex_list and len(vertex_list) != 1:
                vertex_list = set(vertex_list) - {"*"}
                for vertex in vertex_list:
                    output.append((tupples[0][0], tupples[0][1], vertex))

        return output
    x=1
    # print(f'triangles:{generateTriplets(output)}')
    return list(generateTriplets(output))


def driverNodeIteratorAlgorithm(edges):
    return findTriangles(edges)


if __name__ == '__main__':
    global edges, vectices
    spark = SparkSession \
        .builder.master('local[*]') \
        .appName("example-spark").getOrCreate()
    # combined = spark.read.format(formatter).options(delimiter=',', header='false', inferSchema=True) \
    #     .load('edges.csv').withColumnRenamed('_c0', 'src').withColumnRenamed('_c1', 'dst').withColumnRenamed('_c2',
    #                                                                                                             'probs')
    # edges = combined.dropDuplicates(['src', 'dst'])
    #
    # vdf = (combined.select(combined['src']).union(combined.select(combined['dst']))).distinct()
    #
    # # create a dataframe with only one column
    # new_vertices = vdf.select(vdf['src'].alias('id')).distinct()
    #

    edges = spark.createDataFrame([(1, 2, 0.3), (1, 3, 0.3), (2, 3, 0.4),
                                   (3, 4, 0.3), (3, 5, 0.3), (4, 5, 0.3),
                                   (2, 4, 0.3), (4, 6, 0.3), (5, 6, 0.3), (1, 5, 0.3), (2, 5, 0.5)])
    vectices = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,), (6,)])
    edges = edges

    import time

    currentMilliTime = lambda: int(round(time.time() * 1000))

    t1 = currentMilliTime()
    output1 = driverNodeIteratorAlgorithm(edges)
    # print("No. of Triangles:\t", output1)
    t2 = currentMilliTime()
    time1 = t2 - t1
    print("NodeIterator Algorithm's Execution Time: \t\t {0} milliseconds.".format(time1))
    spark.stop()
