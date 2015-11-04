<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [UgridContext class](#ugridcontext-class)
- [ugrid.init_cb([callback])](#ugrid-init_cb-callback)
- [ugrid.parallelize(localArray, [P])](#ugrid-parallelize-localarray-p)
- [ugrid.textFile(file, [P])](#ugrid-textfile-file-p)
- [ugrid.randomSVMData(N, D, [seed, P])](#ugrid-randomsvmdata-n-d-seed-p)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## UgridContext class
Example:

	var UgridContext = require('ugrid-context');
	var ugrid = new UgridContext({host: 'localhost', port: 12346});

or in a shorter form:

	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});

## ugrid.init_cb([callback])
Example:

	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});
	ugrid.init(function () {
		console.log('init done, ugrid ready');
	});

Or with using co:

	var co = require('co');
	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});

	co(function *() {
		yield ugrid.init();
		console.log('init done, ugrid ready');
	})();

## ugrid.parallelize(localArray, [P])

	var V = [1, 2, 3, 4];
	var data = ugrid.parallelize(V);

## ugrid.textFile(file, [P])

	var file = 'data.txt';
	var data = ugrid.textFile(file);

## ugrid.randomSVMData(N, D, [seed, P])

	var N = 1000;
	var D = 4;
	var data = ugrid.randomSVMData(N, D);



