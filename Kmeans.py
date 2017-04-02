"""
A K-means clustering program using MLlib.
This example requires NumPy (http://www.numpy.org/).
"""
#Reference : https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/kmeans.py
from __future__ import print_function

import sys

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


if __name__ == "__main__":

    kmeans_dt8 = open("/home/hadoop/output_assignment2/Kmeans_clustering_output.txt",'w+')
    filespath = sys.argv[1]
    k_val = sys.argv[2]
    sc = SparkContext(appName="KMeans")
    lines = sc.textFile(filespath)
    data = lines.map(parseVector)
    k = k_val
    model = KMeans.train(data, k)
    print("Final centers: " + str(model.clusterCenters))
    print("Total Cost: " + str(model.computeCost(data)))
    kmeans_dt8.write("The Mean sqaured error = ")
    kmeans_dt8.write("Final centers: " + str(model.clusterCenters))
    kmeans_dt8.write(str(model.computeCost(data)))

    sc.stop()

