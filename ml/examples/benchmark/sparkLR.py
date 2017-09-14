"""
Simple Logistic regression algorithm.

This example requires NumPy (http://www.numpy.org/).
"""

from math import exp
from math import sqrt
import sys
import math

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD

D = 16                  # Number of features
weights = np.zeros(D)   # initial weights

def parsePoint(line):
    values = [float(s) for s in line.split(' ')]
    return [values[0], values[1:]]

def logisticLossGradient(point):
    label = -point[0]
    features = point[1]
    dot = 0
    grad = []
    for i in range(0, D):
        dot += features[i] * weights[i]
    t2 = exp(label * dot)
    tmp = t2 / (1 + t2) * label
    for i in range(0, D):
        grad += [features[i] * tmp]
    return grad

def mySum(a, b):
    for i in range (0, D):
        a[i] += b[i]
    return a

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: sparkLR <file> <iterations>"
        exit(-1)

    sc = SparkContext(appName="pysparkLR")
    points = sc.textFile(sys.argv[1]).map(parsePoint).persist()

    N = points.count()
    
    iterations = int(sys.argv[2])
    for i in range(0, iterations):
        gradient = points.map(logisticLossGradient).reduce(mySum)
        for j in range(0, D):
            weights[j] -= gradient[j] / (N * sqrt(i + 1))
    # format and output weights to stdout  
    line = str(weights[0])
    for i in range (1, D):
        line += " "  + str(weights[i])
    sys.stdout.write(line + "\n")
    sc.stop()
