# Machine Learning module

The Machine Learning (ML) module provides scalable functions for
supervised (classification, regression) and unsupervised (clustering)
statistical learning on top of skale datasets and distributed
map-reduce engine.

The module can be loaded using:

```javascript
var ml = require('skale-engine/ml')
```

## binaryClassificationMetrics(measures, options[, done])

## KMeans(nbClusters[, options])

Creates a clusterization model fitted via [K-Means] algorithm.

- *nbClusters*: *Number*, specifying the number of clusters in the model
- *Options*: an optional *Object* with the following fields:
  - *maxMse*: *Number* defining the maximum mean square error between cluster
    centers since previous iteration. Used to stop iterations. Default to 1e-7.
  - *maxIterations*: *Number* defining the maximum number of iterations. Default: 100.
  - *means*: an initial array of vectors (arrays) of numbers, default undefined.

Example:

```javascript
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
dataset *trainingSet*. The result is passed to the *done()* callback
if provided, otherwise an [ES6 promise] is returned.

- *trainingSet*: a dataset where entries are in the following format:
  `[feature0, feature1, ...]` with *featureN* being a float number.
- *done*: an optional callback of the form `function(error, result)`
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

```javascript
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
input dataset *trainingSet*. The result is passed to the *done()*
callback if provided, otherwise an [ES6 promise] is returned.

- *trainingSet*: a dataset where entries are in the following format:
  `[label, [feature0, feature1, ...]]` with *label* being either 1 or -1,
  and *featureN* being a float number, preferentially with a zero mean and
  unit variance (in range [-1, 1]). Sparse vectors with undefined features
  are supported.
- *done*: an optional callback of the form `function(error, result)`
  which is called at completion.

### sgd.predict(sample)

Predict a label for a given *sample* and returns a numerical value
which can be converted to a label -1 if negative, or 1 if positive.

If selected loss is `log`, the returned value can be interpreted as
a probability of the corresponding label.

## StandardScaler()

### scaler.fit(dataset, [done])

### scaler.transform(features)

[readable stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[ES6 promise]: https://promisesaplus.com
[action]: #actions
[K-Means]: https://en.wikipedia.org/wiki/K-means_clustering
[loss function]: https://en.wikipedia.org/wiki/Loss_functions_for_classification
[logistic regression]: https://en.wikipedia.org/wiki/Logistic_regression
[ml.StandardScaler]: #mlstandardscaler
[parquet]: https://parquet.apache.org
[regularization]: https://en.wikipedia.org/wiki/Regularization_(mathematics)
[stochastic gradient descent]: https://en.wikipedia.org/wiki/Stochastic_gradient_descent
[support vector machine]: https://en.wikipedia.org/wiki/Support_vector_machine
