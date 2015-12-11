/* ugrid framework */

'use strict';

var ugrid = {};
module.exports = ugrid;

ugrid.Context = require('./lib/ugrid-context.js');

ugrid.ml = require('./lib/ugrid-ml.js');

ugrid.context = function(opt) {return new ugrid.Context(opt);};

ugrid.onError = function (err) {
	console.error(err.stack);
	process.exit(1);
};
