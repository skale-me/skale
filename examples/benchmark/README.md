# Logistic regression with Skale

## Prerequisites

To run logreg.ipynb you need to:

- install skale toolbelt
- install Jupyter Notebook
- install iJavascript kernel for Jupyter

To install skale toolbelt, run

	sudo npm install -g skale
	
To install Jupyter notebook follow those steps.

To install iJavascript kernel follow those steps.

NB: It may be good to pack everything inside a docker image.

## Running the Jupyter notebook

First start skale local cluster:

	cd examples/benchmark
	skale start
	
Now that skale local cluster is running, to run the Jupyter Notebook, enter

	ijs logreg.ipynb 

## Running the application with skale toolbelt
To run the application with skale toolbelt, simply enter
	
	cd examples/benchmark
	skale run