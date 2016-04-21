#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Logistic regression using MLlib.

This example requires NumPy (http://www.numpy.org/).
"""

from math import exp
import sys
import math

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
import numpy as np

D = 16 # Number of dimensions
seed = 1

def parsePoint(line):
    """
    Parse a line of text into an MLlib LabeledPoint object.
    """
    values = [float(s) for s in line.split(' ')]
    if values[0] == -1:   # Convert -1 labels to 0 for MLlib
        values[0] = 0
    return LabeledPoint(values[0], values[1:])

def random_sample():
	global seed
	x = math.sin(seed) * 10000;
	seed = seed + 1;
	return (x - math.floor(x)) * 2 - 1;
	
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: logistic_regression <file> <iterations>"
        exit(-1)
    w = np.zeros(D)
  #   for i in range(0,D):	
		# w[i] =  random_sample() 
    # print "Initial w: " + str(w)

    sc = SparkContext(appName="PythonLR")
    points = sc.textFile(sys.argv[1]).map(parsePoint).persist()
    iterations = int(sys.argv[2])
    model = LogisticRegressionWithSGD.train(points, iterations, step=1.0, miniBatchFraction=1.0, initialWeights = w, regParam = 0, regType = None, intercept=False, validateData = False)
    line = str(model.weights[0])
    for i in range (1, D):
        line += " "  + str(model.weights[i])
    sys.stdout.write(line + "\n")
    sc.stop()
