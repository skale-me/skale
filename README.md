# Skale Machine Learning Library (skale-ml) Guide

skale-ml is Skale’s machine learning library. Its goal is to make practical machine learning scalable, easy and accessible to javascript and node.js developers. It consists of common learning algorithms and utilities, including classification, regression and clustering.

skale-ml is currently in alpha mode version.

## I - Classification

### Logistic regression
Logistic regression is a linear method generally used to predict binary as well ass multiclass responses. 

For now, skale-ml only support the binary logistic regression. The generelization into multinomial logistic regression needs to be implemented.

#### example
In the example folder you can find a sample skale application manipulating the logistic regression model.

First, if not already done, install globally [skale toolbelt](https://www.npmjs.com/package/skale):

```
npm install -g skale
```

Then clone [skale-ml](https://github.com/skale-me/skale-ml) locally on your laptop using the following command

```
git clone https://github.com/skale-me/skale-ml.git
```

Navigate to the logistic regression example application folder, intall dependencies and run your the test application.

```
cd /path/to/skale-ml/examples/logreg
npm install
skale run
```

The logreg example generates a random Support Vector Machine dataset using skale-ml.RandomSVMData source. It then trains a binary logistic regression model using a Stochastic Gradient Descent on a given number of iterations. The duration of each iteration computation is being displayed. As skale is processing in-memory later iterations are much faster than the first one.

### Linear Support Vector Machines (SVMs)

To be documented

---

## II - Regression

### Linear least squares, Lasso, and ridge regression
To be documented


## III - Clustering

### K-means
To be documented
