#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

try {
	co(function *() {

		yield ugrid.init();

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
		console.log(res)
		ugrid.end();
	})();
} catch (err) {
	json.error = err;
	process.exit(1);
}
