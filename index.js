/* ugrid framework */

'use strict';

var thenify = require('thenify').withCallback;
var ugrid = {};
module.exports = ugrid;

ugrid.Context = require('./lib/ugrid-context.js');

ugrid.ml = require('./lib/ugrid-ml.js');

ugrid.context = thenify(function context(opt, callback) {
	var uc = new ugrid.Context(opt);
	uc.init(opt, callback);
});

ugrid.onError = function (err) {
	console.error(err.stack);
	process.exit(1);
};

