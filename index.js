/* skale framework */

'use strict';

var Context = require('./lib/context.js');
var Dataset = require('./lib/dataset.js');

module.exports = {
	Context: Context,
	context: Context,
	HashPartitioner: Dataset.HashPartitioner,
	RangePartitioner: Dataset.RangePartitioner,
	Random: Dataset.Random,
	Source: Dataset.Source
};
