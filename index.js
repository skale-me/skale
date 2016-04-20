/* skale framework */

'use strict';

const Context = require('./lib/context.js');
const Dataset = require('./lib/dataset.js');

module.exports = {
	Context: Context,
	context: Context,
	HashPartitioner: Dataset.HashPartitioner,
	RangePartitioner: Dataset.RangePartitioner,
	Random: Dataset.Random,
	Source: Dataset.Source
};
