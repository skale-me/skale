# Skale Hacker's Guide

## Introduction

Skale is a fast and general purpose distributed data processing system. It provides a high-level API in Javascript and an optimized parallel execution engine.

This document gives an overview of its design and architecture, then some details on internals and code organisation, and finally presents how to extends various parts of the engine.

It is assumed that the reader is already familiar with using skale and with the [reference guide](skale-API.md), at least the [core concepts](skale-API.md#core-concepts).

## Architecture

This section describes the core architecture of skale. At high level, a skale application consists of a *master* program which launches various parallel tasks on *worker* nodes. The tasks read and write *datasets*, i.e. arrays of data of abritrary size, split in *partitions* distributed on workers.

### Master

The corresponding code is in [context-local.js](../lib/context-local.js) for the standalone mode, or [context.js](../lib/context.js) for the distributed mode, the only difference between the 2 being the way workers are created and connected to the master.

In a nutshell, the master performs the following:

1. Creates a new skale [context](../lib/context.js#L22) object to hold the state of cluster, datasets and tasks, then in this context:
2. Allocates a new cluster, i.e. and array of [workers](../lib/context.js#L51-L53): connected slave processes on each worker host (1 process per CPU).
3. [Compiles then run](../lib/context.js#L223) an execution graph from the user code, the *job*, consisting of a sequence of *stages*. This compilation is only triggered when an *action* is met, thus in *lazy* mode.
4. For each stage, [runs the next task](../lib/context.js#L129): serialize and send stage code and metadata about input dataset partitions to the next free worker, trigger execution, wait for result, repeat until all stage's tasks are completed.

*Stage explanation here*

### Worker

The corresponding code is in [worker-local.js](../lib/worker-local.js) for the standalone mode and [worker.js](../bin/worker.js) for the distributed mode. The common part is implemented in [task.js](../lib/task.js).

A worker performs the following:

1. Connects to the master and wait for the next task to execute, then for each task:
2. Select input partition(s), possible cases are:
   - in memory local partition computed from a previous stage, already loaded
   - on-disk local partition computed from a previous stage, spilled to disk
   - remote partition stored on a separate worker (post-shuffle)
   - external data source, through a source connector
3. Iterate on partition(s), applying for each record a *pipeline* of functions as defined by the user for the current stage (for example a filter function, followed by a mapper function, followed by a reducer function)
4. The last function of the pipeline is either an *action* (function returning data to master), or a pre-shuffle function (saving data on disk for remote access at start of next stage, i.e post-shuffle)
5. At end of task, a result is sent to master, usually metadata for output files, used for next stage or for final combiner action

*Explain communication model here, for data transfers and remote procedure calls*

### Datasets

The main abstraction provided by skale is a *dataset* which is similar to a Javascript array, but partitioned accross the workers that can be operated in parallel.

A dataset object is always created first on the master side, either by a *source* function which returns a dataset from an external input or from scratch, or by a *transformation* function, which takes a dataset in input and outputs a new dataset.

The same code, in [dataset.js](../lib/dataset.js) is loaded both in master and workers. A dataset object instantiated on master will be replicated on each worker through task [serialization](../lib/context.js#L141) and [deserialization](../bin/worker.js#L275) process.

From an object oriented perspective, all *sources* and *transformations*, as dataset contructors, are classes which derive and inherit from the *Dataset* class, whereas *actions*, which operate on a dataset object, are simply methods of the *Dataset* class.

Dataset objects have methods that can be run either on master side or on worker side (never on both), the following table provides a summary of these:

|Dataset method     | on master | on worker | type | description |
|-------------------|-----------|-----------|------|-------------|
|getPartitions      | ✓ |   | source, post-shuffle transform| Allocate output dataset partitions |
|getPreferedLocation| ✓ |   | source                        | return prefered worker for a given partition |
|iterate            |   | ✓ | source, post-shuffle transform| iterate stage pipeline on partition entries|
|transform          |   | ✓ | transform                     | Apply a custom operation on each input dataset entry, pre-shuffle|
|spillToDisk        |   | ✓ | pre-shuffle transform         | dump partition data to disk during pre-shuffle, for next stage|

### Local standalone mode

The standalone local mode limits the scalability to the single machine but simplifies the use, as it is only necessary to `require('skale-engine')`, and avoid cluster or extra server and configuaration management.

In local standalone mode, the workers processes are created on the same host as the master, by the master itself, using the NodeJS core [cluster](https://nodejs.org/dist/latest-v7.x/docs/api/cluster.html) module.

### Distributed mode

## Adding a new source

A source returns a dataset from an external input or from scratch. For example, to be able to process data from kafka in a parallel manner, i.e. one topic partition per worker, one has to implement a kafka source in skale.

Adding a new source is a matter of:

- Deriving a new class from the Dataset class, see as for example [TextLocal](../lib/dataset.js#L911-L919), which implements a textFile source from local filesystem
- Providing a `getPartition` method prototype, which allocates a fixed number of partitions, see [TextLocal.getPartitions](../lib/dataset.js#L921-L941) as an example of allocating one partition per file. This method will be run on the master, when triggered by the action, and prior to dispatch tasks to workers
- Optionally providing a `getPreferedLocation` method prototype, to select a given worker according to your source semantics. If not provided, the master will dispatch the partition by default to the next free worker at execution time.
- Providing an `iterate` method prototype, which operates this time on the worker to execute the stage pipeline on each partition entry. See for example [TextLocal.iterate](../lib/dataset.js#943) and [iterateStream](../lib/dataset.js#L800) which processes each line of a [readable stream](https://nodejs.org/api/stream.html#stream_class_stream_readable). If the partition can be mapped to a readable stream, as it is the case for many NodeJS connectors, one can just reuse `iterateStream` as is.
- Exposing the source in the API, either by extending [textFile](../lib/context.js#L112-121) to process a new URL protocol, or adding a new source method in the context, see for example [parallelize](../lib/context.js#L107).

## Adding a new transform

A new transform can be implemented either by deriving a new class from the Dataset class then providing dataset methods as in the previous table of dataset methods, or by composing existing tranform methods to issue a new one, see for example [distinct](../lib/dataset.js#L121-L125).

*Here give details on narrow vs wide transforms and impact on implementation*

## Adding a new action
