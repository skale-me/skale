#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.range(5).map(a => 2*a).save('/tmp/truc', function (err) {
	if (err) console.log(err);
	sc.end();
});
