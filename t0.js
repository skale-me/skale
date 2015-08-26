'use strict';

var trace = require('line-trace');
var Cluster = require('./lib/ugrid-cluster.js');

var test = new Cluster('test', 14);

console.log(test.cluster);
test.start(function (err, res) {
//test.stop(function (err, res) {
	console.log("cluster started");
});
