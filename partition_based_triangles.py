import itertools
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession

formatter = 'com.databricks.spark.csv'


def distinct_triangles(triangles):
    unique_triangles_set = set()
    unique_triangles_list = []
    for triang in triangles:
        tmp = frozenset([triang[0][0], triang[0][1],
                        triang[1][0], triang[1][1],
                       triang[2][0], triang[2][1]])
        if tmp not in unique_triangles_set:
            unique_triangles_set.add(tmp)
            unique_triangles_list.append(triang)
    return unique_triangles_list


def find_top_triangles(triangles):
    TOP_NUMBER = 20
    weight_tring = []
    for triang in triangles:
        weight = triang[0][2] * triang[1][2] * triang[2][2]
        weight_tring.append((triang, weight))
    sorted_list = sorted(weight_tring, key=lambda x: x[1], reverse = True)
    return sorted_list[:TOP_NUMBER]


def spark_init():
    sparkConf = SparkConf().setMaster("local[1]")
    spark = SparkSession \
        .builder \
        .appName("Partition") \
        .config(conf=sparkConf) \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def my_mapper(edge):
    """
    Partitioning the graphs into sub_graphs using python processes
    :param edge: instance of edgelist
    :return: the output are nodes that will be served to reducers for parallel purposes
    """
    processes = 4
    # ftiaxnoume ena ena key px '0 1 3' kai se ayto to key dinoume ena
    node_1 = int(edge[0]) % processes
    node_2 = int(edge[1]) % processes
    output = []

    for a in range(0, processes):
        for b in range(a + 1, processes):
            for c in range(b + 1, processes):
                if {node_1, node_2}.issubset({a, b, c}):
                    # for example ('0 1 3', [(3, 5)])
                    (output.append((str(a) + " " + str(b) + " " + str(c), tuple([(edge[0], edge[1], edge[2])]))))
    return output


def my_reducer(edges):
    return find_triangles(edges)


def find_triangles(edges):
    """

    :param edges:
    :return: yield the triangles for each mapper
    """
    mean_prob = sum([edge[2] for edge in edges]) / len(edges)
    result = []
    done = set()
    # for each edge we have: src = edge[0], dst = edge[1], prob = edge[2]
    for n in edges:
        done.add(n)
        nbrdone = set()
        # if edge has propability less than mean_prob continue
        if (n[2] < mean_prob):
            continue
        nbrs = tuple([edge for edge in edges if edge[0] == n[0] or edge[1] == n[0]])
        if len(nbrs) < 2:
            continue

        for nbr in nbrs:
            if nbr in done:
                continue
            nbrdone.add(nbr)
            # if edge has propability less than mean_prob continue
            if (nbr[2] < mean_prob):
                continue
            # third_node have as src or dst the node n or the node nbr. So we need to check for both of them
            # first we check nbr[1] == third_node[0] and n[1] == third_node[1]
            third_node = None
            for edge in edges:
                if nbr[1] == edge[0] and n[1] == edge[1]:
                    third_node = edge
                    break

            if third_node is not None and (third_node not in done and third_node not in nbrdone):
                result.append((n, nbr, third_node))
            else:
                # we check nbr.dst == third_node and n.dst == third_node.dst
                # third_node = tuple([edge; break for edge in edges if edge[0] == n[1] and edge[1] == nbr[1])
                for edge in edges:
                    if edge[0] == n[1] and edge[1] == nbr[1]:
                        third_node = edge
                        break
                if third_node is not None and (third_node not in done and third_node not in nbrdone):
                    result.append((n, nbr, third_node))
    return result


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


def main():
    """
    Driver with mapper and  reducer
    :return: Most probable triangles yielded
    """
    spark, sc = spark_init()
    edges, vertices = get_edges(spark)
    currentMilliTime = lambda: int(round(time.time() * 1000))
    t1 = currentMilliTime()

    mapper_output = (edges.rdd.flatMap(lambda edge: my_mapper(edge))  # moirase tis akmes se 4 sinola
        .reduceByKey(
        lambda key, edge: key + edge))  # enose sta sinola tis akmes (px to 1o sinolo na pari tis akmes x,y,z)

    reducer_output = mapper_output.map(lambda edges: my_reducer(edges[1]))

    triangles = [i for i in reducer_output.toLocalIterator()]
    triangles = set(itertools.chain.from_iterable(triangles))

    [print(i) for i in triangles]
    print('-' * 100)
    print(len(triangles))

    triangles = distinct_triangles(triangles)
    top_triangles = find_top_triangles(triangles)

    print("-" * 100)
    [print(i) for i in top_triangles]
    t2 = currentMilliTime()
    time2 = t2 - t1
    print(time2)
    spark.stop()


if __name__ == '__main__':
    main()
