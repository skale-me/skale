## Introduction

Skale is a fast and general purpose distributed data processing
system. It provides a high-level API in Javascript and an optimized
parallel execution engine.

A Skale application consists of a *master* program that runs the
user code and executes various *parallel operations* on a cluster
of *workers*.

The main abstraction Skale provides is a *dataset* which is similar
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

*Sources* and *transformations* in Skale are *lazy*. They do not
start right away, but are triggered by *actions*, thus allowing
efficient pipelined execution and optimized data transfers.

A first example:

```javascript
var sc = require('skale').context();		// create a new context
sc.parallelize([1, 2, 3, 4]).				// source
   map(function (x) {return x+1}).			// transform
   reduce(function (a, b) {return a+b}, 0).	// action
   then(console.log);						// process result: 14
```

## Core concepts

As stated above, a program can be considered as a workflow of steps,
each step consisting of a transformation which inputs from one or
more datasets (parents), and outputs to a new dataset (child).

### Partitioning

Datasets are divided into several partitions, so each partition can
be assigned to a separate worker, and processing can occur concurently
in a distributed and parallel system.

The consequence of this partitioning is that two types of transformations
exist:

- *Narrow* transformations, where each partition of the parent dataset
  is used by at most one partition of the child dataset. This is the
  case for example for `map()` or `filter()`, where each dataset entry
  is processed independently from each other.
  Partitions are decoupled, no synchronization
  between workers is required, and narrow transformations can be
  pipelined on each worker.

- *Wide* transformations, where multiple child partitions may depend
  on one parent partition. This is the case for example for `sortBy()`
  or `groupByKey()`. Data need to be exchanged between workers or
  *shuffled*, in order to complete the transformation. This introduces
  synchronization points which prevent pipelining.

### Pipeline stages and shuffles

Internally, each wide transformation consists of a pre-shuffle and
a post-shuffle part. All sequences of steps from source to pre-shuffle,
or from post-shuffle to next pre-shuffle or action, are thus only
narrow transformations, or pipelined stages (the most efficient
pattern).  A skale program is therefore simply a sequence of stages
and shuffles, shuffles being global serialization points.

It's important to grab this concept as it sets the limit to the
level of parallelism which can be achieved by a given code.

The synoptic table of [transformations](#transformations) indicates
for each transformation if it is narrow or wide (shuffle).

## Working with datasets

### Sources

After having initialized a cluster context using [skale.context()],
one can create a dataset using the following sources:

| Source Name                 | Description                                            |
| ----------------------------| ------------------------------------------------------ |
|[lineStream(stream)]         | Create a dataset from a text stream                    |
|[objectStream(stream)]       | Create a dataset from an object stream                 |
|[parallelize(array)]         | Create a dataset from an array                         |
|[range(start,end,step)]      | Create a dataset containing integers from start to end |
|[source(size,callback,args)] | Create a dataset from a custom source function         |
|[textFile(path, options)]    | Create a dataset from text file                        |

### Transformations

Transformations operate on a dataset and return a new dataset. Note that some
transformation operate only on datasets where each element is in the form
of 2 elements array of key and value (`[k,v]` dataset):

	[[Ki,Vi], ..., [Kj, Vj]]

A special transformation `persist()` enables one to *persist* a dataset
in memory, allowing efficient reuse accross parallel operations.

|Transformation Name               |Description                                                            |In         |Out          |Shuffle|
|----------------------------------|-----------------------------------------------------------------------|-----------|-------------|-------|
|[aggregateByKey(func, func, init)]|Reduce and combine by key using functions                              |[k,v]      |[k,v]        |yes    |
|[cartesian(other)]                |Perform a cartesian product with the other dataset                     |v w        |[v,w]        |yes    |
|[coGroup(other)]                  |Group data from both datasets sharing the same key                     |[k,v] [k,w]|[k,[[v],[w]]]|yes    |
|[distinct()]                      |Return a dataset where duplicates are removed                          |v          |w            |yes    |
|[filter(func)]                    |Return a dataset of elements on which function returns true            |v          |w            |no     |
|[flatMap(func)]                   |Pass the dataset elements to a function which returns a sequence       |v          |w            |no     |
|[flatMapValues(func)]             |Pass the dataset [k,v] elements to a function without changing the keys|[k,v]      |[k,w]        |no     |
|[groupByKey()]                    |Group values with the same key                                         |[k,v]      |[k,[v]]      |yes    |
|[intersection(other)]             |Return a dataset containing only elements found in both datasets       |v w        |v            |yes    |
|[join(other)]                     |Perform an inner join between 2 datasets                               |[k,v]      |[k,[v,w]]    |yes    |
|[leftOuterJoin(other)]            |Join 2 datasets where the key must be present in the other             |[k,v]      |[k,[v,w]]    |yes    |
|[rightOuterJoin(other)]           |Join 2 datasets where the key must be present in the first             |[k,v]      |[k,[v,w]]    |yes    |
|[keys()]                          |Return a dataset of just the keys                                      |[k,v]      |k            |no     |
|[map(func)]                       |Return a dataset where elements are passed through a function          |v          |w            |no     |
|[mapValues(func)]                 |Map a function to the value field of key-value dataset                 |[k,v]      |[k,w]        |no     |
|[reduceByKey(func, init)]         |Combine values with the same key                                       |[k,v]      |[k,w]        |yes    |
|[partitionBy(partitioner)]        |Partition using the partitioner                                        |v          |v            |yes    |
|[persist()]                       |Idempotent, keep content of dataset in cache for further reuse         |v          |v            |no     |
|[sample(rep, frac)]               |Sample a dataset, with or without replacement                          |v          |w            |no     |
|[sortBy(func)]                    |Sort a dataset                                                         |v          |v            |yes    |
|[sortByKey()]                     |Sort a [k,v] dataset                                                   |[k,v]      |[k,v]        |yes    |
|[subtract(other)]                 |Remove the content of one dataset                                      |v w        |v            |yes    |
|[union(other)]                    |Return a dataset containing elements from both datasets                |v          |v w          |no     |
|[values()]                        |Return a dataset of just the values                                    |[k,v]      |v            |no     |

### Actions

Actions operate on a dataset and send back results to the *master*. Results
are always produced asynchronously and send to an optional callback function,
alternatively through a returned [ES6 promise].

| Action Name                      |Description                                                       |out                |
|----------------------------------|------------------------------------------------------------------|-------------------|
|[aggregate(func, func, init)]     |Similar to reduce() but may return a different typei              |value              |
|[collect()]                       |Return the content of dataset                                     |array of elements  |
|[count()]                         |Return the number of elements from dataset                        |number             |
|[countByKey()]                    |Return the number of occurrences for each key in a `[k,v]` dataset|array of [k,number]|
|[countByValue()]                  |Return the number of occurrences of elements from dataset         |array of [v,number]|
|[first()]                         |Return the first element in dataset i                             |value              |
|[forEach(func)]                   |Apply the provided function to each element of the dataset        |empty              |
|[lookup(k)]                       |Return the list of values `v` for key `k` in a `[k,v]` dataset    |array of v         |
|[reduce(func, init)]              |Aggregates dataset elements using a function into one value       |value              |
|[save(url)]                       |Save the content of a dataset to an url                           |empty              |
|[stream()]                        |Stream out a dataset                                              |stream             |
|[take(num)]                       |Return the first `num` elements of dataset                        |array of value     |
|[takeSample(withReplacement, num)]|Return a sample of `num` elements of dataset                      |array of value     |
|[top(num)]                        |Return the top `num` elements of dataset                          |array of value     |

[ES6 promise]: https://promisesaplus.com
[skale.context()]: skale-API.md#skalecontextconfig

[lineStream(stream)]: skale-API#sclinestreaminput_stream
[objectStream(stream)]: skale-API#scobjectstreaminput_stream
[parallelize(array)]: skale-API#scparallelizearray
[range(start,end,step)]: skale-API#scrangestart-end-step
[source(size,callback,args)]: skale-API#scsourcesize-callback-args
[textFile(path, options)]: skale-API#sctextfilepath-options

[aggregateByKey(func, func, init)]: skale-API#dsaggregatebykeyreducer-combiner-init-obj
[cartesian(other)]: skale-API#dscartesianother
[coGroup(other)]: skale-API#dscogroupother
[distinct()]: skale-API#dsdistinct
[filter(func)]: skale-API#dsfilterfilter-obj
[flatMap(func)]: skale-API#dsflatmapflatmapper-obj
[flatMapValues(func)]: skale-API#dsflatmapvaluesflatmapper-obj
[groupByKey()]: skale-API#dsgroupbykey
[intersection(other)]: skale-API#dsintersectionother
[join(other)]: skale-API#dsjoinother
[leftOuterJoin(other)]: skale-API#dsleftouterjoinother
[rightOuterJoin(other)]: skale-API#dsrightouterjoinother
[keys()]: skale-API#dskeys
[map(func)]: skale-API#dsmapmapper-obj
[mapValues(func)]: skale-API#dsmapvaluesmapper-obj
[reduceByKey(func, init)]: skale-API#dsreducebykeyreducer-init-obj
[partitionBy(partitioner)]: skale-API#dspartitionbypartitioner
[persist()]: skale-API#dspersist
[sample(rep, frac)]: skale-API#dssamplewithreplacement-frac
[sortBy(func)]: skale-API#dssortbykeyfunc-ascending
[sortByKey()]: skale-API#dssortbykeyascending
[subtract(other)]: skale-API#dssubtractother
[union(other)]: skale-API#dsunionother
[values()]: skale-API#dsvalues

[aggregate(func, func, init)]: skale-API#dsaggregatereducer-combiner-init-obj-done
[collect()]: skale-API#dscollectdone
[count()]: skale-API#dscountdone
[countByKey()]: skale-API#dscountbykeydone
[countByValue()]: skale-API#dscountbyvaluedone
[first()]: skale-API#dsfirstdone
[forEach(func)]: skale-API#dsforeachcallback-obj-done
[lookup(k)]: skale-API#dslookupk-done
[reduce(func, init)]: skale-API#dsreducereducer-init-obj-done
[save(url)]: skale-API#dssaveurl-options-done
[stream()]: skale-API#dsstreamopt
[take(num)]: skale-API#dstakenum-done
[takeSample(withReplacement, num)]: skale-API#dstakesamplewithreplacement-num-done
[top(num)]: skale-API#dstopnum-done
