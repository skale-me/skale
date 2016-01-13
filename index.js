/* ugrid framework */

'use strict';

var ugrid = {};
module.exports = ugrid;

ugrid.Context = require('./lib/ugrid-context.js');

ugrid.ml = require('./lib/ugrid-ml.js');

ugrid.context = ugrid.Context;

ugrid.HashPartitioner = require('./lib/ugrid-array.js').HashPartitioner;

ugrid.RangePartitioner = require('./lib/ugrid-array.js').RangePartitioner;

ugrid.onError = function (err) {
	console.error(err.stack);
	uc.end();
};
