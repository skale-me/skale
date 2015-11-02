# Ugrid Reference

<!-- toc -->
- [Overview](#overview)
- [Working with distributed arrays](#working_with_da)
- [Ugrid module](#ugrid_module)
  * [ugrid.context([config])](#ugrid-context)
  * [uc.end()](#uc-end)
  * [uc.parallelize(array)](#uc-parallelize)
  * [uc.randomSVMData(nb_entries, dimension, seed)](#uc-randomsvmdata)
  * [uc.textFile(path)](#uc-textfile)
  * [uc.lineStream(input_stream, config)](#uc-linestream)
  * [uc.objectStream(input_stream, config)](#uc-objectstream)
- [Distributed Arrays](#distributed-arrays)
  * [da.map(mapper [, obj])](#da-map)
  * [da.flatMap(flatMapper [, obj])](#da-flatmap)
  * [da.mapValues(mapper [, obj])](#da-mapvalues)

<!-- tocstop -->

## [Overview](id:overview)
Ugrid is a fast and general purpose distributed data processing system. It provides a high-level API in Javascript and an optimized parallel execution engine.

A Ugrid application consist of a *master* program that runs the user code and executes various *parallel operations* on a cluster of *workers*.

The main abstraction Ugrid provides is a *distributed array* (DA) which is similar to a Javascript *array*, but partitioned accross the workers that can be operated in parallel. Think to DAs as Javascript arrays on steroids, with no limits on size and scalability.

There are several ways to create a DA: *parallelizing* an existing array in the master program, or referencing a dataset in a distributed storage system (such as HDFS), or *streaming* the content of any source that can be processed through Node.js *Streams*. We call *source* a function which initializes a DA.

DAs support two kinds of operations: *transformations*, which create a new distributed array from an existing one, and *actions*, which return a value to the *master* program after running a computation on the DA (all DA content remains in workers memory and/or storage, master stores only DA metadata).

For example, `map` is a transformation that applies a function to each element of a DA, returning a new DA. On the other hand, `reduce` is an action that aggregates all elements of a DA using some function, and returns the final result to the master.

*Sources* and *transformations* in Ugrid are *lazy*. They do not start right away, but are triggered by *actions*, thus allowing efficient pipelined execution and optimized data transfers.

A first example:

```
var uc = require('ugrid').context();		// create a new context
uc.parallelize([1, 2, 3, 4])				// source
  .map(function (x) {return x+1})			// transform
  .reduce(function (a, b) {return a+b}, 0)	// action
  .then(console.log);						// process result: 14
```

## [Working with Distributed Arrays](id:working_with_da)

After having initialized a cluster context using [ugrid.context()](#ugrid-context), one can create a distributed array
using the following sources:

Source Name                                       | Description 
--------------------------------------------------|--------------------------------------
[uc.parallelize(array)](#uc-parallelize)          | creates a DA from an array
[uc.textFile(path)](#uc-textfile)	              | creates a DA from a regular text file
[uc.lineStream(stream)](#uc-linestream)           | creates a DA from a text stream
[uc.objectStream(stream)](#uc-objectstream)       | creates a DA from an object stream
[uc.randomSVMData(N, D, seed)](#uc-randomsvmdata) | creates a DA from random data

Transformations:

Transformation Name | Description | in | out
--------------------|-------------|-------|------
[da.map(func)](#da.map)|Apply a function on each element of DA| v | V
[da.flatMap(func)](#da.flatmap)| | v | [V]
[da.mapValues(func)](#da.flatmap)| | [k, v] | [k, V]

Actions:

Action Name | Description | Returns
-----|-------------|--------


## [Ugrid module](id:ugrid_module)
The Ugrid module is the main entry point for Ugrid functionality. To use it, one must `require('ugrid')`.


### [ugrid.context([config])](id:ugrid-context)
Creates and returns a new context which represents the connection to the Ugrid cluster, and which can be
used to create DAs on that cluster. Config is an *Object* which defines the cluster server, with the following defaults:

```
{
  host: 'localhost',	// Cluster server host, settable also by UGRID_HOST env
  port: '12346'			// Cluster server port, settable also by UGRID_PORT env
}
```

Example:

```
var ugrid = require('ugrid');
var uc = ugrid.context();
```

#### [uc.end()](id:uc-end)
Closes the connection to the cluster.

#### [uc.parallelize(array)](id:uc-parallelize)
Returns a new DA containing elements from the *Array* array.

Example:

```
var a = uc.parallelize(['Hello', 'World']);
```

#### [uc.randomSVMData(nb_entries, dimension, seed)](id:uc-randomsvmdata)
- nb_entries *Number* - total number of entries
- dimension *Number* - number of feature values per entry
- seed *Number* - value of random seed

Returns a new DA containig random support vector machine data (see https://en.wikipedia.org/wiki/Support_vector_machine), suitable for machine learning tests.

Each entry is an array where the first element is a label with a value of -1 or 1, and the second element is an array of random numerical values between -1 and 1, the features.

Example:

```
uc.randomSVMData(3, 2, 0).collect().toArray().then(console.log)
// [ [ -1, [ 0.9485365136351902, -0.5998388026555403 ] ],
//   [ 1, [ 0.5145067372322956, 0.690036021483138 ] ],
//   [ 1, [ 0.16493246763639036, -0.6302951648685848 ] ] ]
```

#### [uc.textFile(path)](id:uc-textfile)
Returns a DA of lines composing the file specified by path *String*.

Note: If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

Example, the following program prints the length of a text file:

```
var lines = uc.textFile('data.txt');
lines.map(s => s.length).reduce((a, b) => a + b, 0).then(console.log);
```

#### [uc.lineStream(input_stream)](id:uc-linestream)
Returns a DA of lines of text read from input_stream *Object*, which is a [readable stream](https://nodejs.org/api/stream.html#stream_class_stream_readable) where DA content is read from.

Example:

```
var stream = fs.createReadStream('data.txt', 'utf8');
uc.lineStream(stream).map(s => s.length).reduce((a, b) => a + b, 0).then(console.log);
```

#### [uc.objectStream(input_stream)](id:uc-objectstream)
Returns a DA of Javascript *Objects* read from input_stream *Object*, which is a [readable stream](https://nodejs.org/api/stream.html#stream_class_stream_readable) where DA content is read from.

The following example counts the number of objects returned in an object stream using the mongodb native Javascript driver:

```
var cursor = db.collection('clients').find();
uc.objectStream(cursor).count().then(console.log);
```

Users may also *persist* a DA in memory, allowing efficient reuse accross parallel operations.

### Working with Key-Values Distributed Arrays

A Key-values distributed array is structured as follow:

	[[Ki, Vi], … [Kj, Vj]]

It can be seen as an arbitrary-sized Javascript Array where each element is a Javascript Array containing 2 elements, the key and the value, which can be of any serializable type. The developper must ensure that data passed to transformations working on key-value pairs are well-structured.

All sources are methods of UgridContext and return a new DA.

## Transformations

Transformations are methods of the DA class. They all operate on a DA and return a new DA, so they can be chained. A transformation can take the following parameters:

- A DA callback  function, called for each element. The helper function must be self-contained, or rely on dependencies accessible through the worker context (see below).
- An additional data object, which will be passed to the helper function. Those data must be serializable (it must be possible to apply `JSON.stringify()` on it)

DA callback function has a form of `function helper(element, [[data] [, wc]])`, where:

- *element* is the next element of the DA on which the transformation operates.
- *data* is the user additional data as passed to the transformation. It must be serializable.
- *wc* is the worker context, a global object defined in each worker and persistent accross transformations. It can be used to extend the worker capabilities through `wc.require()`.

Example:
```
var uc = require('ugrid').context();

function mapper(element, data, wc) {
	if (!wc.maxmind) wc.maxmind = wc.reqire('maxmind');
	return wc.maxmind.getCountry(element);
}
var res = uc.parallelize(vect).map(mapper).collect();
```

Following is the detailed description of each transformation.

### [da.map(mapper [, obj])](id:da-map)
 - *mapper*: a function of the form `callback(element [[,obj] [, wc]])`, returning an element and where:
   - *element*: the next element of the DA on which `map()` operates
   - *obj*: the same parameter *obj* passed to `map()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided mapper function to each element of the source DA and returns a new DA.

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

The following example program

```
var uc = require('ugrid').context();

function mapper(data, obj) { return data * obj.scaling }

var res = uc.parallelize([1, 2, 3, 4])
	.map(mapper, {scaling: 1.2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);
// 
```

will display

```
1.2
2.4
3.6
4.8
```

### [da.flatMap(flatMapper [, obj])](id:da-flatmap)

Applies the provided mapper function to each element of the source DA and returns a new DA.

   - *mapper*: a function, defined as function `flatMapper(data [[,obj] [, wc]])`, to be applied to each element of the dataset and which returns an array of elements
   - *obj*: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function flatMapper(data, obj) {
	    var tmp = [];
	    for (var i = 0; i < obj.N; i++) tmp.push(data);
	    return tmp;
	    }
	    uc.parallelize([1, 2, 3, 4])
		    .flatMap(flatMapper, {N: 2})
		    .collect(function(err, res) {
			    if (err) {console.log(err); process.exit();}
		    console.log(res);
		    uc.end();
		});
    })

### DA.mapValues(mapper [, obj])**

Applies the provided mapper function to the value of each [key, value] element of the source DA
and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

### ** Arguments:**

   - mapper: a function, defined as function mapper(data [[,obj] [, wc]]), to be applied to each element of the dataset and which returns a single element, conserving the key of source elements
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function valueMapper(data, obj) {
	    return data * obj.fact;
	    }
	    uc.parallelize([['hello', 1], ['world', 2]])
		    .mapValues(valueMapper, {fact: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
    uc.end();
	    });
    })

* ##**DA.flatMapValues(flatMapper [, obj])**

Applies the provided flatMapper function to the value of each [key, value] element of the source
DA and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

### ** Arguments:**

   - mapper: a function, defined as function flatMapper(data [[,obj] [, wc]]), to be applied to each element of the dataset and which returns an array of elements, conserving the key of source elements
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function valueFlatMapper(data, obj) {
	    var tmp = [];
	    for (var i = 0; i < obj.N; i++)
	    tmp.push(data * obj.fact);
	    return tmp;
	    }
	    uc.parallelize([['hello', 1], ['world', 2]])
		    .flatMapValues(valueFlatMapper, {N: 2, fact: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
	    uc.end();
	    });
    })

* ##**DA.filter(filter [, obj])**

Applies the provided filter function to each element of the source DA and returns a new DA
containing the elements that passed the test.

### ** Arguments:**

   - filter: a function, defined as function filter(data [[,obj] [, wc]]), to test each element of the dataset and which returns true or false
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function filter(data, obj) {
	    return data % obj.modulo;
	    }
	    uc.parallelize([1, 2, 3, 4])
		    .filter(filter, {modulo: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
	    uc.end();
	    });
    });

* Supported transformations, not yet documented
*    sample(withReplacement, frac, seed)
*    groupByKey()
*    reduceByKey()
*    union()
*    join(other)
*    leftOuterJoin(other)
*    rightOuterJoin(other)
*    coGroup(other)
*    crossProduct(other)
*    intersection(other)
*    subtract(other)
*    keys()
*    values()
*    Actions
*    aggregate()
*    reduce()
*    collect()
*    count()
*    forEach()
*    lookup(key)
*    countByValue()
*    countByKey()
