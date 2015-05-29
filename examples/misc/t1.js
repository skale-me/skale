#!/usr/bin/env node

var grid = require('../../lib/ugrid-client.js')({
	data: {
		type: 't1',
		query: {type: 'worker-controller'}
	}
}, function (err, res) {
	console.log(res);
});
