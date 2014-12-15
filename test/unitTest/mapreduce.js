#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

try {
	co(function *() {

		yield grid.connect();
		var res = yield grid.send('devices', {type: "worker"});
		var ugrid = new UgridContext(grid, res[0].devices);

		var N = 4;
		var D = 2;

		function mapper(a) {
			var o = JSON.parse(JSON.stringify(a));
			o.new_field = "mapped";
			return o;
		}

		function reducer(a, b) {
			for (var i = 0; i < b.features.length; i++)
				a.features[i] += b.features[i];
			return a;
		}

		var res = yield ugrid.loadTestData(N, D).map(mapper, []).reduce(reducer, {label: 1, features: ml.zeros(D)});

		grid.disconnect();
	})();
} catch (err) {
	json.error = err;
	console.log(JSON.stringify(json));
	process.exit(1);
}
