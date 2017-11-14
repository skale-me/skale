# Machine Learning module

The Machine Learning (ML) module provides scalable functions for
supervised (classification, regression) and unsupervised (clustering)
statistical learning on top of skale datasets and distributed
map-reduce engine.

The module can be loaded using:
```js
var ml = require('skale/ml')
```

## classificationMetrics(measures[, options][, done])

This [action] computes various metrics to measure classification performance.

- *measures*: a dataset where entries are in the form
  `[prediction, label]` with *prediction* and *label* being numbers where
  only value sign is used: true if positive or false if negative.
- *options*: an optional *Object* with the following fields:
    - *steps*: integer *Number* defining the number of points in the Receiver
      Operation Charateristics (ROC) curve. Defaults: 10
- *done*:  an optional callback of the form `function(error, result)`
  which is called at completion. *result* is an object with the following fields:
    - *rates*: an array of *steps* of confusion matrix raw values
    - *auROC*: area under ROC curve, using the trapezoidal rule
    - *auPR*: area under Precision Recall curve, using the trapezoidal rule

Example:
```js
var model = new ml.SGDLinearModel();
await model.fit(trainingSet);
var predictionAndLabels = testSet.map((p, model) => [model.predict(p[1]), p[0]], model);
var metrics = await ml.classificationMetrics(predictionAndLabels)
console.log('ROC AUC:', metrics.auROC);
// 0.869
```

## KMeans(nbClusters[, options])

Creates a clusterization model fitted via [K-Means] algorithm.

- *nbClusters*: *Number*, specifying the number of clusters in the model
- *Options*: an optional *Object* with the following fields:
    - *maxMse*: *Number* defining the maximum mean square error between cluster
      centers since previous iteration. Used to stop iterations. Default to 1e-7.
    - *maxIterations*: *Number* defining the maximum number of iterations. Default: 100.
    - *means*: an initial array of vectors (arrays) of numbers, default undefined.

Example:
```js
const dataset = sc.parallelize([
  [1, 2], [1, 4], [1, 0],
  [4, 2], [4, 4], [4, 0]
]);
const kmeans = ml.KMeans(2);
await kmeans.fit(dataset);
kmeans.means
// [ [ 2.5, 1 ], [ 2.5, 4 ] ]
kmeans.predict([0, 0])
// 0
kmeans.predict([4, 4]
// 1
```

### kmeans.fit(trainingSet[, done])

This [action] updates *kmeans* model by fitting it to the input
dataset *trainingSet*. The *done()* callback is called at completion
if provided, otherwise an [ES6 promise] is returned.

- *trainingSet*: a dataset where entries are in the following format:
  `[feature0, feature1, ...]` with *featureN* being a float number.
- *done*: an optional callback of the form `function(error)`
  which is called at completion.

### kmeans.predict(sample)

Returns the closest cluster index for the *sample*.

- *sample*: an *Array* with the format `[feature0, feature 1, ...]`
  with *featureN* being a float number.

## SGDLinearModel([options])

Creates a regularized linear model fitted via [stochastic
gradient descent] learning. Such model can be used either for 
regression or classification, as training method is identical,
only prediction changes. SGD is sensitive to the scaling
of the features. For best results, the data should have zero mean and
unit variance, which can be achieved with [ml.StandardScaler].

The model it fits can be controlled with the *loss* option; by default,
it fits a linear [support vector machine] (SVM). A regularization term
can be added to the loss, by default the squared euclidean norm L2.

- *options*: an *Object* with the following fields:
  - *fitIntercept*: *Boolean* indicating whether to include an intercept. Default: *true*
  - *loss*: *String* specifying the [loss function] to be used. Possible values are:
      - `hinge`: (default), gives a linear SVM
      - `log`: gives logistic loss, a probabilistic classifier
      - `square`: gives square loss fit
  - *penalty*: *String*  specifying the [regularization] term. Possible values are:
      - `l2`: (default) squared euclidean norm L2, standard regularizer for linear SVM models
      - `l1`: absolute norm L1, might bring sparsity to the model, not achievable with `l2`
      - `none`: zero penalty
  - *proba*: *Boolean* (default *false*). If *true* predict returns a probability rather than a raw number. Only applicable when logisitic loss is selected.
  - *regParam*: *Number*  >= 0, defaults to 0.001, defines the trade-off between the
    two goals of minimizing the loss (i.e. training error) and minimizing model complexity
    (i.e. to avoid overfitting)
  - *stepSize*: *Number* >= 0, defaults to 1, defines the initial step size of the gradient
    descent

Example:
```js
const trainingSet = sc.parallelize([
 [1, [0.5, -0.7]],
 [-1, [-0.5, 0.7]]
]);
const sgd = new ml.SGDLinearModel()
await sgd.fit(trainingSet, 2)
sgd.weights
// [ 0.8531998372026804, -1.1944797720837526 ]
sgd.predict([2, -2])
// 0.9836229103782058
```

### sgd.fit(trainingSet, iterations[, done])

This [action] updates *sgdClassifier* model by fitting it to the
input dataset *trainingSet*. The *done()* callback is called at
completion if provided, otherwise an [ES6 promise] is returned.

- *trainingSet*: a dataset where entries are in the following format:
  `[label, [feature0, feature1, ...]]` with *label* being either 1 or -1,
  and *featureN* being a float number, preferentially with a zero mean and
  unit variance (in range [-1, 1]). Sparse vectors with undefined features
  are supported.
- *done*: an optional callback of the form `function(error)`
  which is called at completion.

### sgd.predict(sample)

Predict a label for a given *sample* and returns a numerical value
which can be converted to a label -1 if negative, or 1 if positive.

If selected loss is `log`, the returned value can be interpreted as
a probability of the corresponding label.

## StandardScaler()

Creates a standard scaler which standardizes features by removing
the mean and scaling to unit variance.

Centering and scaling happen independently on each feature by
computing the relevant statistics on the samples in the training
set. 

Standardization of datasets is a common requirement for many machine
learning estimators. They might behave badly if the individual
features do not more or less look like standard normally distributed
data: Gaussian with zero mean and unit variance.

Example:
```js
var data = sc.parallelize([[0, 0], [0, 0], [1, 1], [1, 1]]);
var scaler = new ml.StandardScaler();
await scaler.fit(data);
scaler
//StandardScaler {
//  transform: [Function],
//  count: 4,
//  mean: [ 0.5, 0.5 ],
//  std: [ 0.5, 0.5 ] }
var scaled = data.map((p, scaler) => scaler.transform(p), scaler)
console.log(await scaled.collect());
// [ [ -1, -1 ], [ -1, -1 ], [ 1, 1 ], [ 1, 1 ] ]
scaler.transform([2, 2])
// [ 3, 3 ]
```

### scaler.fit(trainingSet[, done])

This [action] updates *scaler* by computing the mean and std of
*trainingSet* to be used for later scaling. The *done()* callback
is called at completion if provided, otherwise an [ES6 promise] is
returned.

- *trainingSet*: a dataset where entries are in the format
  `[feature0, feature1, ...]` with *featureN* being a *Number*
- *done*: an optional callback of the form `function (error)` which
  is called at completion.

### scaler.transform(sample)

Returns the standardized scaled value of *sample*.

- *sample*: an *Array* with the format `[feature0, feature 1, ...]`
  with *featureN* being a float number.

[readable stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[ES6 promise]: https://promisesaplus.com
[action]: concepts#actions
[K-Means]: https://en.wikipedia.org/wiki/K-means_clustering
[loss function]: https://en.wikipedia.org/wiki/Loss_functions_for_classification
[logistic regression]: https://en.wikipedia.org/wiki/Logistic_regression
[ml.StandardScaler]: #mlstandardscaler
[parquet]: https://parquet.apache.org
[regularization]: https://en.wikipedia.org/wiki/Regularization_(mathematics)
[stochastic gradient descent]: https://en.wikipedia.org/wiki/Stochastic_gradient_descent
[support vector machine]: https://en.wikipedia.org/wiki/Support_vector_machine
