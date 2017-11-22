// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

const ContextRemote = require('./lib/context.js');
const ContextLocal = require('./lib/context-local.js');
const Dataset = require('./lib/dataset.js');

function Context(args) {
  args = args || {};
  if (args.host || process.env.SKALE_HOST) return ContextRemote(args);
  return ContextLocal(args);
}

module.exports = {
  Context: Context,
  context: Context,
  HashPartitioner: Dataset.HashPartitioner,
  RangePartitioner: Dataset.RangePartitioner,
  Source: Dataset.Source
};
