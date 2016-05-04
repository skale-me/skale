#!/usr/bin/env node

const sc = require('skale-engine').context();

//sc.range(5).forEach((a, b) => console.log('# b', b), () => {
sc.range(5).forEach((b) => console.log('# b', b), () => {
	console.log('done');
	sc.end();
});
