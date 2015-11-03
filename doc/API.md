# Ugrid Reference

<!-- toc -->
- [Overview](#overview)
- [Working with Distributed Arrays](#working-with-distributed-arrays)
- [Ugrid module](#ugrid-module)
  * [ugrid.context([config])](#ugridcontext--config--)
    + [uc.end()](#ucend--)
    + [uc.parallelize(array)](#ucparallelize-array-)
    + [uc.textFile(path)](#uctextfile-path-)
    + [uc.lineStream(input_stream)](#uclinestream-input-stream-)
    + [uc.objectStream(input_stream)](#ucobjectstream-input-stream-)
  * [Distributed Arrays methods](#distributed-arrays-methods)
    + [da.map(mapper [, obj])](#damap-mapper----obj--)
    + [da.flatMap(flatMapper [, obj])](#daflatmap-flatmapper----obj--)
    + [da.mapValues(mapper [, obj])](#damapvalues-mapper----obj--)
    + [da.flatMapValues(flatMapper [, obj])](#daflatmapvalues-flatmapper----obj--)
    + [da.filter(filter [, obj])](#dafilter-filter----obj--)

<!-- tocstop -->

## Overview

Ugrid is a fast and general purpose distributed data processing system. It provides a high-level API in Javascript and an optimized parallel execution engine.

A Ugrid application consist of a *master* program that runs the user code and executes various *parallel operations* on a cluster of *workers*.

The main abstraction Ugrid provides is a *distributed array* (DA) which is similar to a Javascript *array*, but partitioned accross the workers that can be operated in parallel.

There are several ways to create a DA: *parallelizing* an existing array in the master program, or referencing a dataset in a distributed storage system (such as HDFS), or *streaming* the content of any source that can be processed through Node.js *Streams*. We call *source* a function which initializes a DA.

DAs support two kinds of operations: *transformations*, which create a new distributed array from an existing one, and
*actions*, which return a value to the *master* program after running a computation on the DA.

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

## Working with Distributed Arrays

After having initialized a cluster context using [ugrid.context()](#ugrid-context), one can create a distributed array
using the following sources:

Source Name                                       | Description 
--------------------------------------------------|--------------------------------------
[uc.lineStream(stream)](#uclinestream)            | Create a DA from a text stream
[uc.objectStream(stream)](#ucobjectstream)        | Create a DA from an object stream
[uc.parallelize(array)](#ucparallelize)           | Create a DA from an array
[uc.textFile(path)](#uctextfile)	              | Create a DA from a regular text file

Transformations operate on a DA and return a new DA. Note that some transformation operate only on DA where each element
is in the form of 2 elements array of key and value (KV DA): `[[Ki,Vi], ..., [Kj, Vj]]`

Transformation Name | Description | in | out
--------------------|-------------|-------|------
[da.cartesian(other)](#dacartesian) | Cartesian product with the other DA | v w | [v,w]
[da.coGroup(other)](#dacogroup) | Group data from both DAs sharing the same key | [k,v] [k,w] | [k,[[v],[w]]]
[da.distinct()](#dadistinct)    | Return a DA where duplicates are removed | v | w
[da.filter(func)](#dafilter)    | Return a DA of elements on which function returns true | v | w
[da.flatMap(func)](#daflatmap)  | Similar to map(), but where function returns a sequence of elements | v | w
[da.groupByKey()](#dagroupbykey)| Group values with the same key | [k,v] | [k,[v]]
[da.intersection(other)](#daintersection) | Return a DA containing only elements found in both DAs | v w | v
[da.join(other)](#dajoin)       | Perform an inner join between 2 DAs | [k,v] | [k,[v,w]]
[da.leftOuterJoin(other)](#daleftouterjoin) | Perform a join between 2 DAs where the key must be present in the other DA | [k,v] | [k,[v,w]]
[da.rightOuterJoin(other)](#darightouterjoin) | Perform a join between 2 DAs where the key must be present in the first DA | [k,v] | [k,[v,w]]
[da.keys()](#dakeys)            | Return a DA of just the keys | [k,v] | k
[da.map(func)](#damap)          | Return a DA where elements are passed through a function | v | w
[da.mapValues(func)](#daflatmap)| Similar to map(), but where function is applied to value of key-value DA | [k,v] | [k,w]
[da.reduceByKey(func, init)](#dareducebykey)	| Combine values with the same key | [k,v] | [k,w]
[da.persist()](#dapersist)      | Idempotent. Keep content of DA in cache for further reuse. | v | v
[da.sample(withReplacement, frac, seed)](#dasample) | Sample a DA, with or without replacement | v | w
[da.subtract(other)](#dasubract) | Remove the content of one DA | v w | v
[da.union(other)](#daunion)     | Return a DA containing elements from both DAs | v | v w
[da.values()](#davalues)        | Return a DA of just the values | [k,v] | v

Actions:

Action Name | Description | out
-----|-------------|---
[da.aggregate(func, func, init)](#daaggregate)| Similar to reduce() but may return a different type| value
[da.collect()](#dacollect)         | Return the content of DA | stream of elements
[da.count()](#dacount)             | Return the number of elements from DA | number
[da.countByKey()](#dacountbykey)     | Return the number of occurrences of elements for each key in a KV DA | stream of [k,number]
[da.countByValue()](#dacountbyvalue) | Return the number of occurrences of elements from DA | stream of [v,number]
[da.foreach(func)](#daforeach)     | Apply the provided function to each element of the DA | null
[da.lookup(k)](#dalookup)          | Return the list of values for key k in a KV DA | stream of v
[da.reduce(func, init)](#dareduce) | Apply a function against an accumulator and each element of DA, return a single value | value

## Ugrid module

The Ugrid module is the main entry point for Ugrid functionality. To use it, one must `require('ugrid')`.


### ugrid.context([config])

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

#### uc.end()

Closes the connection to the cluster.

#### uc.parallelize(array)

Returns a new DA containing elements from the *Array* array.

Example:

```
var a = uc.parallelize(['Hello', 'World']);
```

#### uc.textFile(path)

Returns a DA of lines composing the file specified by path *String*.

Note: If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

Example, the following program prints the length of a text file:

```
var lines = uc.textFile('data.txt');
lines.map(s => s.length).reduce((a, b) => a + b, 0).then(console.log);
```

#### uc.lineStream(input_stream)

Returns a DA of lines of text read from input_stream *Object*, which is a [readable stream](https://nodejs.org/api/stream.html#stream_class_stream_readable) where DA content is read from.

Example:

```
var stream = fs.createReadStream('data.txt', 'utf8');
uc.lineStream(stream).map(s => s.length).reduce((a, b) => a + b, 0).then(console.log);
```

#### uc.objectStream(input_stream)

Returns a DA of Javascript *Objects* read from input_stream *Object*, which is a [readable stream](https://nodejs.org/api/stream.html#stream_class_stream_readable) where DA content is read from.

The following example counts the number of objects returned in an object stream using the mongodb native Javascript driver:

```
var cursor = db.collection('clients').find();
uc.objectStream(cursor).count().then(console.log);
```

Users may also *persist* a DA in memory, allowing efficient reuse accross parallel operations.

### Distributed Arrays methods

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

#### da.map(mapper [, obj])

 - *mapper*: a function of the form `callback(element [[,obj] [, wc]])`, returning an element and where:
   - *element*: the next element of the DA on which `map()` operates
   - *obj*: the same parameter *obj* passed to `map()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided mapper function to each element of the source DA and returns a new DA.

The following example program

```
var uc = require('ugrid').context();

function mapper(data, obj) { return data * obj.scaling }

var res = uc.parallelize([1, 2, 3, 4])
	.map(mapper, {scaling: 1.2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);
```

will display

```
1.2
2.4
3.6
4.8
```

#### da.flatMap(flatMapper [, obj])

 - *flatMapper*: a function of the form `callback(element [[,obj] [, wc]])`, returning an *Array* and where:
   - *element*: the next element of the DA on which `flatMap()` operates
   - *obj*: the same parameter *obj* passed to `flatMap()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided mapper function to each element of the source DA and returns a new DA.

Example:

```js
function flatMapper(data, obj) {
	var tmp = [];
	for (var i = 0; i < obj.N; i++) tmp.push(data);
	return tmp;
}

uc.parallelize([1, 2, 3, 4])
  .flatMap(flatMapper, {N: 2})
  .collect().on('data', console.log);
```

#### da.mapValues(mapper [, obj])

 - *mapper*: a function of the form `callback(element [[,obj] [, wc]])`, returning an element and where:
   - *element*: the value v of the next [k,v] element of the DA on which `mapValues()` operates
   - *obj*: the same parameter *obj* passed to `mapValues()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided mapper function to the value of each [key, value] element of the source DA
and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

Example:

```js
function valueMapper(data, obj) { return data * obj.fact; }

uc.parallelize([['hello', 1], ['world', 2]])
  .mapValues(valueMapper, {fact: 2})
  .collect().on('data', console.log)
```

#### da.flatMapValues(flatMapper [, obj])

 - *flatMapper*: a function of the form `callback(element [[,obj] [, wc]])`, returning an *Array* and where:
   - *element*: the value v of the next [k,v] element of the DA on which `flatMapValues()` operates
   - *obj*: the same parameter *obj* passed to `flatMapValues()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided flatMapper function to the value of each [key, value] element of the source
DA and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

Example:

```js
function valueFlatMapper(data, obj) {
	var tmp = [];
	for (var i = 0; i < obj.N; i++) tmp.push(data * obj.fact);
	return tmp;
}

uc.parallelize([['hello', 1], ['world', 2]])
  .flatMapValues(valueFlatMapper, {N: 2, fact: 2})
  .collect().on('data', console.log);
```

#### da.filter(filter [, obj])

 - *filter*: a function of the form `callback(element [[,obj] [, wc]])`, returning a *Boolean* and where:
   - *element*: the next element of the DA on which `filter()` operates
   - *obj*: the same parameter *obj* passed to `filter()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

Applies the provided filter function to each element of the source DA and returns a new DA
containing the elements that passed the test.

Example:

```js
function filter(data, obj) { return data % obj.modulo; }

uc.parallelize([1, 2, 3, 4])
  .filter(filter, {modulo: 2})
  .collect().on('data', console.log);
```

Supported transformations, not yet documented:

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

Actions:

*    aggregate()
*    reduce()
*    collect()
*    count()
*    forEach()
*    lookup(key)
*    countByValue()
*    countByKey()
