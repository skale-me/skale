/* skale framework */

'use strict';

var skale = {};
module.exports = skale;

skale.Context = require('./lib/context.js');

skale.ml = require('./lib/ml.js');

skale.context = skale.Context;

skale.HashPartitioner = require('./lib/dataset.js').HashPartitioner;

skale.RangePartitioner = require('./lib/dataset.js').RangePartitioner;

skale.onError = function (err) {
	console.error(err.stack);
	uc.end();
};
