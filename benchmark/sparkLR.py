"""
Simple Logistic regression algorithm.
"""

from math import exp
from math import sqrt
import sys
import math

from pyspark import SparkContext

stepSize = 1
regParam = 1
D = 16                             # Number of features
weights = [0 for i in range(D)]    # Initial weights

def parsePoint(line):
    values = [float(s) for s in line.split(' ')]
    return [values[0], values[1:]]

def logisticLossGradient(point):
    grad = []
    dotprod = 0
    label = point[0]
    features = point[1]
    for i in range(0, D):
        dotprod += features[i] * weights[i]
    tmp = 1 / (1 + exp(-dotprod)) - label
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
        iterStepSize = stepSize / sqrt(i + 1)
        for j in range(0, D):
            weights[j] -= iterStepSize * (gradient[j] / N + regParam * weights[j])
    # format and output weights to stdout  
    line = str(weights[0])
    for i in range (1, D):
        line += " "  + str(weights[i])
    sys.stdout.write(line + "\n")
    sc.stop()
