# Skale Hacker's Guide

## Introduction

Skale is a fast and general purpose distributed data processing system. It provides a high-level API in Javascript and an optimized parallel execution engine.

This document gives an overview of its design and architecture, then some details on internals and code organisation, and finally presents how to extends various parts of the engine.

It is assumed that the reader is already familiar with using skale and with the [reference guide](skale-API.md), at least the [core concepts](skale-API.md#core-concepts).

## Architecture

At high level, a skale application consists of a *master* program which launches various parallel tasks on *worker* nodes.

### Master

The corresponding code is in [context-local.js](../lib/context-local.js) for the standalone mode, or [context.js](../lib/context.js) for the distributed mode, the only difference between the 2 being the way workers are created and connected to the master.

In a nutshell, the master performs the following:

1. Creates a new skale [context](../lib/context.js#L22) object to hold the state of cluster, datasets and tasks, then in this context:
2. Allocates a new cluster, i.e. and array of [workers](../lib/context.js#L51-L53): connected slave processes on each worker host (1 process per CPU).
3. [Compiles then run](../lib/context.js#L223) an execution graph from the user code, the *job*, consisting of a sequence of *stages*. This compilation is only triggered when an *action* is met, thus in *lazy* mode.
4. For each stage, [runs the next task](../lib/context.js#L129): serialize and send stage code and metadata about input dataset partitions to the next free worker, trigger execution, wait for result, repeat until all stage's tasks are completed.

### Worker

The corresponding code is in [worker-local.js](../lib/worker-local.js) for the standalone mode and [worker.js](../bin/worker.js) for the distributed mode. The common part is implemented in [task.js](../lib/task.js).

A worker performs the following:

1. Connects to the master and wait for the next task to execute, then for each task:
2. Select input partition(s), possible cases:
   - in memory local partition computed from a previous stage, already loaded
   - on-disk local partition computed from a previous stage, spilled to disk
   - remote partition stored on a separate worker (post-shuffle)
   - external data source, through a source connector
3. Iterate on partition(s), applying for each record a *pipeline* of functions as defined by the user for the current stage (for example a filter function, followed by a mapper function, followed by a reducer function)
4. The last function of the pipeline is either an *action* (function returning data to master), or a pre-shuffle function (saving data on disk for remote access at start of next stage, i.e post-shuffle)
5. At end of task, a result is sent to master, usually metadata for output files, used for next stage or for final combiner action

### Datasets

The main abstraction Skale provides is a *dataset* which is similar to a Javascript array, but partitioned accross the workers that can be operated in parallel.

### Local standalone mode

The standalone local mode limits the scalability to the single machine but simplifies the use, as it is only necessary to `require('skale-engine')`, and avoid cluster or extra server and configuaration management.

In local standalone mode, the workers processes are created on the same host as the master, by the master itself, using the NodeJS core [cluster](https://nodejs.org/dist/latest-v7.x/docs/api/cluster.html) module.

### Distributed mode

## Adding a new source

## Adding a new transform

## Adding a new action
