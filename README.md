# Skale Machine Learning Library (skale-ml) Guide

skale-ml is Skale’s machine learning library. Its goal is to make practical machine learning scalable, easy and accessible to javascript and node.js developers. It consists of common learning algorithms and utilities, including classification, regression and clustering.

skale-ml is currently in alpha mode version.

## I - Classification

### Logistic regression
Logistic regression is a linear method generally used to predict binary as well ass multiclass responses. 

For now, skale-ml only support the binary logistic regression. The generelization into multinomial logistic regression needs to be implemented.

#### Sample app
In the example folder you will find a sample skale application manipulating the logistic regression model.

First, if not already done, install globally [skale toolbelt](https://www.npmjs.com/package/skale):

```
(sudo) npm install -g skale
```

Then clone [skale-ml](https://github.com/skale-me/skale-ml) locally on your laptop using the following command

```
git clone https://github.com/skale-me/skale-ml.git
```

Navigate to the logistic regression example application folder, intall dependencies and run the app.

```
cd /path/to/skale-ml/examples/logreg
npm install
skale run
```
#### What's happening ?
This example requires skale-engine and skale-ml.

```
var skale = require('skale-engine');
var ml = require('skale-ml');
```
We create a skale context with:

```
var sc = skale.context();
```
And generate a random Support Vector Machine dataset, making it persistent in memory to accelerate model training:

```
var points = ml.randomSVMData(sc, nObservations, nFeatures, seed).persist();

``` 
We then instantiate a logistic regression model associated to the previously created dataset:

```
var model = new ml.LogisticRegression(points);

```
Finally we train the model on a given number of iterations, display the model weights and end the skale session:

```
model.train(nIterations, function() {
	console.log(model.weights);
	sc.end();
});
```
The computation duration of each iteration is being displayed. As skale is processing in-memory data, later iterations are much faster than the first one.

### Linear Support Vector Machines (SVMs)

To be documented

---

## II - Regression

### Linear least squares, Lasso, and ridge regression
To be documented


## III - Clustering

### K-means
To be documented
