/* ugrid framework */

'use strict';

var ugrid = {};
module.exports = ugrid;

ugrid.Context = require('./lib/context.js');

ugrid.context = ugrid.Context;

ugrid.HashPartitioner = require('./lib/dataset.js').HashPartitioner;

ugrid.RangePartitioner = require('./lib/dataset.js').RangePartitioner;
