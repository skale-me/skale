# Ugrid Machine Learning Library

## Overview

Ugrid Machine learning library contains several commun machine learning
algorithms implementations.

## Models

Each algorithm or model is a different class of the machine learning library. We need
to create an instance of the model to use it.

### ml.LogisticRegression(*points*, *features*, *observations*, *initialW*)

we have to create the object to use the model :

var model = new ugrid.ml.LogisticRegression(points, D, N); 

- *points*: a Distributed Array (DA) with N * D points.
- *features*: number of features par observation.
- *observations*: number of observations.
- *initialW*: *features* lenght vector with initial points .

#### Method

- train(*nIterations*,*callback*): 
Trains a logistic regression model using the given parameters
	- *nIterations*: number of iterations.
	- *callback*: a callback of the form `function (*error*)` which
      is called at completion.
	
	
	
#### Property

- w : 
le calcul des points finaux


#### Example

```javascript
var model = new ugrid.ml.LogisticRegression(points, D, N);

model.train(nIterations,function(err){
	console.log(model.w);
	uc.end();	
});
```

### ml.KMeans(*points*, *nClusters*, *initMeans*)

we have to create the object to use the model :

var model = new ugrid.ml.KMeans(points, K, w);

- *points*: a Distributed Array (DA) with N * D points.
- *nClusters*: number of features par observation.
- *initMeans*: *features* lenght vector with initial points.


#### Method

- train(*nIterations*,*callback*):
Trains a k-means model using the given parameters
	- *nIterations*: number of iterations.
	- *callback*: a callback of the form `function (*error*)` which
      is called at completion.

#### Property

- means : 



#### Example

```javascript
var model = new ugrid.ml.KMeans(points, K, w);
model.train(nIterations,function(err){
	console.log(model.means)
	uc.end();
});
```


