/* skale framework */

'use strict';

var ContextRemote = require('./lib/context.js');
var ContextLocal = require('./lib/context-local.js');
var Dataset = require('./lib/dataset.js');

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
  Random: Dataset.Random,
  Source: Dataset.Source
};
