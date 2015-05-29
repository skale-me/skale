/* ugrid framework */

'use strict';

var thenify = require('thenify');
var ugrid = {};
module.exports = ugrid;

ugrid.Context = require('./lib/ugrid-context.js');

ugrid.ml = require('./lib/ugrid-ml.js');

ugrid.context = thenify.withCallback(ugrid.Context);

ugrid.onError = function (err) {
	console.error(err.stack);
	process.exit(1);
};
