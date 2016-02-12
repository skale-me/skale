# Ugrid Reference

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Overview](#overview)
- [Working with datasets](#working-with-datasets)
- [Ugrid module](#ugrid-module)
    - [ugrid.context([config])](#ugridcontextconfig)
        - [uc.end()](#scend)
        - [uc.parallelize(array)](#scparallelizearray)
        - [uc.textFile(path)](#sctextfilepath)
        - [uc.lineStream(input_stream)](#sclinestreaminput_stream)
        - [uc.objectStream(input_stream)](#scobjectstreaminput_stream)
    - [Dataset methods](#datasets-methods)
        - [ds.aggregate(reducer, combiner, init[,obj][,done])](#dsaggregatereducer-combiner-initobjdone)
        - [ds.cartesian(other)](#dscartesianother)
        - [ds.coGroup(other)](#dscogroupother)
        - [ds.collect([opt])](#dscollectopt)
        - [ds.count([callback])](#dscountcallback)
        - [ds.countByKey()](#dscountbykey)
        - [ds.countByValue()](#dscountbyvalue)
        - [ds.distinct()](#dsdistinct)
        - [ds.filter(filter[,obj])](#dsfilterfilterobj)
        - [ds.flatMap(flatMapper[,obj])](#dsflatmapflatmapperobj)
        - [ds.flatMapValues(flatMapper[,obj])](#dsflatmapvaluesflatmapperobj)
        - [ds.foreach(callback[, obj][, done])](#dsforeachcallback-obj-done)
        - [ds.groupByKey()](#dsgroupbykey)
        - [ds.intersection(other)](#dsintersectionother)
        - [ds.join(other)](#dsjoinother)
        - [ds.keys()](#dskeys)
        - [ds.leftOuterJoin(other)](#dsleftouterjoinother)
        - [ds.lookup(k)](#dslookupk)
        - [ds.map(mapper[,obj])](#dsmapmapperobj)
        - [ds.mapValues(mapper[,obj])](#dsmapvaluesmapperobj)
        - [ds.reduce(reducer, init[,obj][,done])](#dsreducereducer-initobjdone)
        - [ds.reduceByKey(reducer, init[, obj])](#dsreducebykeyreducer-init-obj)
        - [ds.rightOuterJoin(other)](#dsrightouterjoinother)
        - [ds.sample(withReplacement, frac, seed)](#dssamplewithreplacement-frac-seed)
        - [ds.subtract(other)](#dssubtractother)
        - [ds.union(other)](#dsunionother)
        - [ds.values()](#dsvalues)
- [References](#references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

Ugrid is a fast and general purpose distributed data processing
system. It provides a high-level API in Javascript and an optimized
parallel execution engine.

A Ugrid application consists of a *master* program that runs the
user code and executes various *parallel operations* on a cluster
of *workers*.

The main abstraction Ugrid provides is a *dataset* which is similar
to a Javascript *array*, but partitioned accross the workers that
can be operated in parallel.

There are several ways to create a dataset: *parallelizing* an existing
array in the master program, or referencing a dataset in a distributed
storage system (such as HDFS), or *streaming* the content of any
source that can be processed through Node.js *Streams*. We call
*source* a function which initializes a dataset.

Datasets support two kinds of operations: *transformations*, which create
a new dataset from an existing one, and *actions*, which
return a value to the *master* program after running a computation
on the dataset.

For example, `map` is a transformation that applies a function to
each element of a dataset, returning a new dataset. On the other
hand, `reduce` is an action that aggregates all elements of a dataset
using some function, and returns the final result to the master.

*Sources* and *transformations* in Ugrid are *lazy*. They do not
start right away, but are triggered by *actions*, thus allowing
efficient pipelined execution and optimized data transfers.

A first example:

```javascript
var uc = require('ugrid').context();		// create a new context
uc.parallelize([1, 2, 3, 4]).				// source
   map(function (x) {return x+1}).			// transform
   reduce(function (a, b) {return a+b}, 0).	// action
   then(console.log);						// process result: 14
```

## Working with datasets
<a name=working-with-datasets></a>

After having initialized a cluster context using
[ugrid.context()](#ugrid-context), one can create a dataset
using the following sources:

| Source Name                                  | Description                          |
| -------------------                          | ----------------------------------   |
|[lineStream(stream)](#uc-linestream-stream)    | Create a dataset from a text stream |
|[objectStream(stream)](#uc-objectstream-stream)| Create a dataset from an object stream |
|[parallelize(array)](#uc-parallelize-array)    | Create a dataset from an array      |
|[textFile(path)](#uc-textfile-path)            | Create a dataset from a regular text file|

Transformations operate on a dataset and return a new dataset. Note that some
transformation operate only on datasets where each element is in the form
of 2 elements array of key and value (`[k,v]` dataset):

	[[Ki,Vi], ..., [Kj, Vj]]

A special transformation `persist()` enables one to *persist* a dataset
in memory, allowing efficient reuse accross parallel operations.

|Transformation Name              | Description                                   | in    | out   |
| -----------------               |-----------------------------------------------|-------|-------|
|[cartesian(other)](#dscartesian) | Perform a cartesian product with the other dataset | v w | [v,w]|
|[coGroup(other)](#dscogroup) | Group data from both datasets sharing the same key | [k,v] [k,w] |[k,[[v],[w]]]|
|[distinct()](#dsdistinct)    | Return a dataset where duplicates are removed | v | w|
|[filter(func)](#dsfilter)    | Return a dataset of elements on which function returns true | v | w|
|[flatMap(func)](#dsflatmap)  | Pass the dataset elements to a function which returns a sequence | v | w|
|[groupByKey()](#dsgroupbykey)| Group values with the same key | [k,v] | [k,[v]]|
|[intersection(other)](#dsintersection) | Return a dataset containing only elements found in both datasets | v w | v|
|[join(other)](#dsjoin)       | Perform an inner join between 2 datasets | [k,v] | [k,[v,w]]|
|[leftOuterJoin(other)](#dsleftouterjoin) | Join 2 datasets where the key must be present in the other | [k,v] | [k,[v,w]]|
|[rightOuterJoin(other)](#dsrightouterjoin) | Join 2 datasets where the key must be present in the first | [k,v] | [k,[v,w]]|
|[keys()](#dskeys)            | Return a dataset of just the keys | [k,v] | k|
|[map(func)](#dsmap)          | Return a dataset where elements are passed through a function | v | w|
|[mapValues(func)](#dsflatmap)| Map a function to the value field of key-value dataset | [k,v] | [k,w]|
|[reduceByKey(func, init)](#dsreducebykey)	| Combine values with the same key | [k,v] | [k,w]|
|[persist()](#dspersist)      | Idempotent. Keep content of dataset in cache for further reuse. | v | v|
|[sample(rep, frac, seed)](#dssample) | Sample a dataset, with or without replacement | v | w|
|[subtract(other)](#dssubract) | Remove the content of one dataset | v w | v|
|[union(other)](#dsunion)     | Return a dataset containing elements from both datasets | v | v w|
|[values()](#dsvalues)        | Return a dataset of just the values | [k,v] | v|

Actions operate on a dataset and return a result to the *master*. Results
are always returned asynchronously. In a case of a single result,
it is returned through a either a callback, or an [ES6 promise].  In the
case of multiple value results, the action returns a [readable
stream].

|Action Name | Description | out|
|------------------             |----------------------------------------------|--------------|
|[aggregate(func, func, init)](#dsaggregate)| Similar to reduce() but may return a different type| value |
|[collect()](#dscollect)         | Return the content of dataset | stream of elements|
|[count()](#dscount)             | Return the number of elements from dataset | number|
|[countByKey()](#dscountbykey)     | Return the number of occurrences for each key in a `[k,v]` dataset | stream of [k,number]|
|[countByValue()](#dscountbyvalue) | Return the number of occurrences of elements from dataset | stream of [v,number]|
|[foreach(func)](#dsforeach)     | Apply the provided function to each element of the dataset | **not implemented**|
|[lookup(k)](#dslookup)          | Return the list of values `v` for key `k` in a `[k,v]` dataset | stream of v|
|[reduce(func, init)](#dsreduce) | Aggregates dataset elements using a function, return a single value | value|

## Ugrid module

The Ugrid module is the main entry point for Ugrid functionality.
To use it, one must `require('ugrid')`.

### ugrid.context([config])

Creates and returns a new context which represents the connection
to the Ugrid cluster, and which can be used to create datasets on that
cluster. Config is an *Object* which defines the cluster server,
with the following defaults:

```javascript
{
  host: 'localhost',	// Cluster server host, settable also by UGRID_HOST env
  port: '12346'			// Cluster server port, settable also by UGRID_PORT env
}
```

Example:

```javascript
var ugrid = require('ugrid');
var uc = ugrid.context();
```

#### uc.end()

Closes the connection to the cluster.

#### uc.parallelize(array)

Returns a new dataset containing elements from the *Array* array.

Example:

```javascript
var a = uc.parallelize(['Hello', 'World']);
```

#### uc.textFile(path)

Returns a dataset of lines composing the file specified by path *String*.

Note: If using a path on the local filesystem, the file must also
be accessible at the same path on worker nodes. Either copy the
file to all workers or use a network-mounted shared file system.

Example, the following program prints the length of a text file:

```javascript
var lines = uc.textFile('data.txt');
lines.map(s => s.length).reduce((a, b) => a + b, 0).then(console.log);
```

#### uc.lineStream(input_stream)

Returns a dataset of lines of text read from input_stream *Object*, which
is a [readable stream] where dataset content is read from.

The following example computes the size of a file using streams:

```javascript
var stream = fs.createReadStream('data.txt', 'utf8');
uc.lineStream(stream).
   map(s => s.length).
   reduce((a, b) => a + b, 0).
   then(console.log);
```

#### uc.objectStream(input_stream)

Returns a dataset of Javascript *Objects* read from input_stream *Object*,
which is a [readable stream] where dataset content is read from.

The following example counts the number of objects returned in an
object stream using the mongodb native Javascript driver:

```javascript
var cursor = db.collection('clients').find();
uc.objectStream(cursor).count().then(console.log);
```

### Dataset methods

Dataset objects, as created initially by above ugrid context source
functions, have the following methods, allowing either to instantiate
a new dataset through a transformation, or to return results to the
master program.

#### ds.aggregate(reducer, combiner, init[,obj][,done])

Returns the aggregated value of the elements of the dataset using two
functions *reducer()* and *combiner()*, allowing to use an arbitrary
accumulator type, different from element type (as opposed to
`reduce()` which imposes the same type for accumulator and element).
The result is passed to the *done()* callback if provided, otherwise
an [ES6 promise] is returned.

- *reducer*: a function of the form `function(acc,val[,obj[,wc]])`,
  which returns the next value of the accumulator (which must be
  of the same type as *acc*) and with:
     - *acc*: the value of the accumulator, initially set to *init*
     - *val*: the value of the next element of the dataset on which
       `aggregate()` operates
     - *obj*: the same parameter *obj* passed to `aggregate()`
     - *wc*: the worker context, a persistent object local to each
       worker, where user can store and access worker local dependencies.
- *combiner*: a function of the form `function(acc1,acc2[,obj])`,
  which returns the merged value of accumulators and with:
     - *acc1*: the value of an accumulator, computed locally on a worker
     - *acc2*: the value of an other accumulator, issued by another worker
     - *obj*: the same parameter *obj* passed to `aggregate()`
- *init*: the initial value of the accumulators that are used by
  *reducer()* and *combiner()*. It should be the identity element
  of the operation (i.e. applying it through the function should
  not change result).
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset
- *done*: a callback of the form `function (error, result)` which
  is called at completion. If *undefined*, `aggregate()` returns
  an [ES6 promise].

The following example computes the average of a dataset, avoiding a `map()`:

```javascript
uc.parallelize([3, 5, 2, 7, 4, 8]).
   aggregate((a, v) => [a[0] + v, a[1] + 1],
		(a1, a2) => [a1[0] + a2[0], a1[1] + a2[1]],
		[0, 0],
		function (err, res) {
			console.log(res[0] / res[1]);
		});
// 4.8333
```

#### ds.cartesian(other)

Returns a dataset wich contains all possible pairs `[a, b]` where `a`
is in the source dataset and `b` is in the *other* dataset.

Example:

```javascript
var ds1 = uc.parallelize([1, 2]);
var ds2 = uc.parallelize(['a', 'b', 'c']);
ds1.cartesian(ds2).collect().toArray().then(console.log);
// [ [ 1, 'a' ], [ 1, 'b' ], [ 1, 'c' ],
//   [ 2, 'a' ], [ 2, 'b' ], [ 2, 'c' ] ]
```

#### ds.coGroup(other)

When called on dataset of type `[k,v]` and `[k,w]`, returns a dataset of type
`[k, [[v], [w]]]`, where data of both datasets share the same key.

Example:

```javascript
var ds1 = uc.parallelize([[10, 1], [20, 2]]);
var ds2 = uc.parallelize([[10, 'world'], [30, 3]]);
ds1.coGroup(ds2).collect().on('data', console.log);
// [ 10, [ [ 1 ], [ 'world' ] ] ]
// [ 20, [ [ 2 ], [] ] ]
// [ 30, [ [], [ 3 ] ] ]
```

#### ds.collect([opt])

Returns a [readable stream] of all elements of the dataset. Optional
*opt* parameter is an object with the default content `{text:
false}`. if `text` option is `true`, each element is passed through
`JSON.stringify()` and a 'newline' is appended, making it possible to
pipe to standard output or any text stream.

Example:

```javascript
uc.parallelize([1, 2, 3, 4]).
   collect({text: true}).pipe(process.stdout);
// 1
// 2
// 3
// 4
```

#### ds.count([callback])

Returns the number of elements in the dataset.

The *callback* is the form of `function(error, result)`, and is
called asynchronously at completion.  In *undefined*, an [ES6
promise] is returned.

Example:

```javascript
uc.parallelize([10, 20, 30, 40]).count().then(console.log);
// 4
```

#### ds.countByKey()

When called on a dataset of type `[k,v]`, computes the number of occurrences
of elements for each key in a dataset of type `[k,v]`. Returns a [readable stream]
of elements of type `[k,w]` where `w` is the result count.

Example:

```javascript
uc.parallelize([[10, 1], [20, 2], [10, 4]]).
   countByKey().on('data', console.log);
// [ 10, 2 ]
// [ 20, 1 ]
```

#### ds.countByValue()

Computes the number of occurences of each element in dataset and returns
a [readable stream] of elements of type `[v,n]` where `v` is the
element and `n` its number of occurrences.

Example:

```javascript
uc.parallelize([ 1, 2, 3, 1, 3, 2, 5 ]).
   countByValue().
   toArray().then(console.log);
// [ [ 1, 2 ], [ 2, 2 ], [ 3, 2 ], [ 5, 1 ] ]
```

#### ds.distinct()

Returns a dataset where duplicates are removed.

Example:

```javascript
uc.parallelize([ 1, 2, 3, 1, 4, 3, 5 ]).
   distinct().
   collect().toArray().then(console.log);
// [ 1, 2, 3, 4, 5 ]
```

#### ds.filter(filter[,obj])

- *filter*: a function of the form `callback(element[,obj[,wc]])`,
  returning a *Boolean* and where:
    - *element*: the next element of the dataset on which `filter()` operates
    - *obj*: the same parameter *obj* passed to `filter()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

Applies the provided filter function to each element of the source
dataset and returns a new dataset containing the elements that passed the
test.

Example:

```javascript
function filter(data, obj) { return data % obj.modulo; }

uc.parallelize([1, 2, 3, 4]).
   filter(filter, {modulo: 2}).
   collect().on('data', console.log);
// 1 3
```

#### ds.flatMap(flatMapper[,obj])

Applies the provided mapper function to each element of the source
dataset and returns a new dataset.

- *flatMapper*: a function of the form `callback(element[,obj[,wc]])`,
  returning an *Array* and where:
    - *element*: the next element of the dataset on which `flatMap()` operates
    - *obj*: the same parameter *obj* passed to `flatMap()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

Example:

```javascript
function flatMapper(data, obj) {
	var tmp = [];
	for (var i = 0; i < obj.N; i++) tmp.push(data);
	return tmp;
}

uc.parallelize([1, 2, 3, 4]).
   flatMap(flatMapper, {N: 2}).
   collect().on('data', console.log);
// [ 'hello', 2 ]
// [ 'hello', 2 ]
// [ 'world', 4 ]
// [ 'world', 4 ]
```

#### ds.flatMapValues(flatMapper[,obj])

Applies the provided flatMapper function to the value of each [key,
value] element of the source dataset and return a new dataset containing
elements defined as [key, mapper(value)], keeping the key unchanged
for each source element.

- *flatMapper*: a function of the form `callback(element[,obj[,wc]])`,
  returning an *Array* and where:
    - *element*: the value v of the next [k,v] element of the dataset on
      which `flatMapValues()` operates
    - *obj*: the same parameter *obj* passed to `flatMapValues()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

Example:

```javascript
function valueFlatMapper(data, obj) {
	var tmp = [];
	for (var i = 0; i < obj.N; i++) tmp.push(data * obj.fact);
	return tmp;
}

uc.parallelize([['hello', 1], ['world', 2]]).
   flatMapValues(valueFlatMapper, {N: 2, fact: 2}).
   collect().on('data', console.log);
```

#### ds.foreach(callback[, obj][, done])

***not implemented***

This action applies a *callback* function on each element of the dataset.

- *callback*: a function of the form `function(val[,obj[,wc]])`,
  which returns *null* and with:
     - *val*: the value of the next element of the dataset on which
       `foreach()` operates
     - *obj*: the same parameter *obj* passed to `foreach()`
     - *wc*: the worker context, a persistent object local to each
       worker, where user can store and access worker local dependencies.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset
- *done*: a callback of the form `function (error, result)` which
  is called at completion. If *undefined*, `foreach()` returns
  an [ES6 promise].

In the following example, the `console.log()` callback provided
to `foreach()` is executed on workers and may be not visible:

```javascript
uc.parallelize([1, 2, 3, 4]).
   foreach(console.log).then(console.log('finished'));
```

#### ds.groupByKey()

When called on a dataset of type `[k,v]`, returns a dataset of type `[k, [v]]`
where values with the same key are grouped.

Example:

```javascript
uc.parallelize([[10, 1], [20, 2], [10, 4]]).
   groupByKey().collect().on('data', console.log);
// [ 10, [ 1, 4 ] ]
// [ 20, [ 2 ] ]
```

#### ds.intersection(other)

Returns a dataset containing only elements found in source dataset and *other*
dataset.

Example:

```javascript
var ds1 = uc.parallelize([1, 2, 3, 4, 5]);
var ds2 = uc.parallelize([3, 4, 5, 6, 7]);
ds1.intersection(ds2).collect().toArray().then(console.log);
// [ 3, 4, 5 ]
```

#### ds.join(other)

When called on source dataset of type `[k,v]` and *other* dataset of type
`[k,w]`, returns a dataset of type `[k, [v, w]]` pairs with all pairs
of elements for each key.

Example:

```javascript
var ds1 = uc.parallelize([[10, 1], [20, 2]]);
var ds2 = uc.parallelize([[10, 'world'], [30, 3]]);
ds1.join(ds2).collect().on('data', console.log);
// [ 10, [ 1, 'world' ] ]
```

#### ds.keys()

When called on source dataset of type `[k,v]`, returns a dataset with just
the elements `k`.

Example:

```javascript
uc.parallelize([[10, 'world'], [30, 3]]).
   keys.collect().on('data', console.log);
// 10
// 30
```

#### ds.leftOuterJoin(other)

When called on source dataset of type `[k,v]` and *other* dataset of type
`[k,w]`, returns a dataset of type `[k, [v, w]]` pairs where the key
must be present in the *other* dataset.

Example:

```javascript
var ds1 = uc.parallelize([[10, 1], [20, 2]]);
var ds2 = uc.parallelize([[10, 'world'], [30, 3]]);
ds1.leftOuterJoin(ds2).collect().on('data', console.log);
// [ 10, [ 1, 'world' ] ]
// [ 20, [ 2, null ] ]
```

#### ds.lookup(k)

When called on source dataset of type `[k,v]`, returns a [readable stream]
of values `v` for key `k`.

Example:

```javascript
uc.parallelize([[10, 'world'], [20, 2], [10, 1], [30, 3]]).
   lookup(10).on('data', console.log);
// world
// 1
```

#### ds.map(mapper[,obj])

Applies the provided mapper function to each element of the source
dataset and returns a new dataset.

- *mapper*: a function of the form `callback(element[,obj[,wc]])`,
  returning an element and where:
    - *element*: the next element of the dataset on which `map()` operates
    - *obj*: the same parameter *obj* passed to `map()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

The following example program

```javascript
uc.parallelize([1, 2, 3, 4]).
   map((data, obj) => data * obj.scaling, {scaling: 1.2}).
   collect().toArray().then(console.log);
// [ 1.2, 2.4, 3.6, 4.8 ]
```

#### ds.mapValues(mapper[,obj])

- *mapper*: a function of the form `callback(element[,obj[,wc]])`,
  returning an element and where:
    - *element*: the value v of the next [k,v] element of the dataset on
      which `mapValues()` operates
    - *obj*: the same parameter *obj* passed to `mapValues()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

Applies the provided mapper function to the value of each `[k,v]`
element of the source dataset and return a new dataset containing elements
defined as `[k, mapper(v)]`, keeping the key unchanged for each
source element.

Example:

```javascript
uc.parallelize([['hello', 1], ['world', 2]]).
   mapValues((a, obj) => a*obj.fact, {fact: 2}).
   collect().on('data', console.log);
// ['hello', 2]
// ['world', 4]
```

#### ds.reduce(reducer, init[,obj][,done])

Returns the aggregated value of the elements of the dataset using a
*reducer()* function.  The result is passed to the *done()* callback
if provided, otherwise an [ES6 promise] is returned.

- *reducer*: a function of the form `function(acc,val[,obj[,wc]])`,
  which returns the next value of the accumulator (which must be
  of the same type as *acc* and *val*) and with:
     - *acc*: the value of the accumulator, initially set to *init*
     - *val*: the value of the next element of the dataset on which
       `reduce()` operates
     - *obj*: the same parameter *obj* passed to `reduce()`
     - *wc*: the worker context, a persistent object local to each
       worker, where user can store and access worker local dependencies.
- *init*: the initial value of the accumulators that are used by
  *reducer()*. It should be the identity element of the operation
  (i.e. applying it through the function should not change result).
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset
- *done*: a callback of the form `function (error, result)` which
  is called at completion. If *undefined*, `reduce()` returns
  an [ES6 promise].

Example:

```javascript
uc.parallelize([1, 2, 4, 8]).
   reduce((a, b) => a + b, 0).
   then(console.log);
// 15
```

#### ds.reduceByKey(reducer, init[, obj])

- *reducer*: a function of the form `callback(acc,val[,obj[,wc]])`,
  returning the next value of the accumulator (which must be of the
  same type as *acc* and *val*) and where:
    - *acc*: the value of the accumulator, initially set to *init*
    - *val*: the value `v` of the next `[k,v]` element of the dataset on
      which `reduceByKey()` operates
    - *obj*: the same parameter *obj* passed to `reduceByKey()`
    - *wc*: the worker context, a persistent object local to each
      worker, where user can store and access worker local dependencies.
- *init*: the initial value of accumulator for each key. Will be
  passed to *reducer*.
- *obj*: user provided data. Data will be passed to carrying
  serializable data from master to workers, obj is shared amongst
  mapper executions over each element of the dataset

When called on a dataset of type `[k,v]`, returns a dataset of type `[k,v]`
where the values of each key are aggregated using the *reducer*
function and the *init* initial value.

Example:

```javascript
uc.parallelize([[10, 1], [10, 2], [10, 4]]).
   reduceByKey((a,b) => a+b, 0).
   collect().on('data', console.log);
// [10, 7]
```

#### ds.rightOuterJoin(other)

When called on source dataset of type `[k,v]` and *other* dataset of type
`[k,w]`, returns a dataset of type `[k, [v, w]]` pairs where the key
must be present in the *source* dataset.

Example:

```javascript
var ds1 = uc.parallelize([[10, 1], [20, 2]]);
var ds2 = uc.parallelize([[10, 'world'], [30, 3]]);
ds1.rightOuterJoin(ds2).collect().on('data', console.log);
// [ 10, [ 1, 'world' ] ]
// [ 30, [ null, 2 ] ]
```

#### ds.sample(withReplacement, frac, seed)

- *withReplacement*: *Boolean* value, *true* if data must be sampled
  with replacement
- *frac*: *Number* value of the fraction of source dataset to return
- *seed*: *Number* value of pseudo-random seed

Returns a dataset by sampling a fraction *frac* of source dataset, with or
without replacement, using a given random generator *seed*.

Example:

```javascript
uc.parallelize([1, 2, 3, 4, 5, 6, 7, 8]).
   sample(true, 0.5, 0).
   collect().toArray().then(console.log);
// [ 1, 1, 3, 4, 4, 5, 7 ]
```

#### ds.subtract(other)

Returns a dataset containing only elements of source dataset which are not
in *other* dataset.

Example:

```javascript
var ds1 = uc.parallelize([1, 2, 3, 4, 5]);
var ds2 = uc.parallelize([3, 4, 5, 6, 7]);
ds1.subtract(ds2).collect().on('data', console.log);
// 1 2
```

#### ds.union(other)

Returns a dataset that contains the union of the elements in the source
dataset and the *other* dataset.

Example:

```javascript
var ds1 = uc.parallelize([1, 2, 3, 4, 5]);
var ds2 = uc.parallelize([3, 4, 5, 6, 7]);
ds1.union(ds2).collect().toArray().then(console.log);
// [ 1, 2, 3, 4, 5, 3, 4, 5, 6, 7 ]
```

#### ds.values()

When called on source dataset of type `[k,v]`, returns a dataset with just
the elements `v`.

Example:

```javascript
uc.parallelize([[10, 'world'], [30, 3]]).
   keys.collect().on('data', console.log);
// 'world'
// 3
```

## References

[ES6 promise]: https://promisesaplus.com

[readable stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
