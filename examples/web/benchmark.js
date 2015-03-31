#!/usr/local/bin/node --harmony

'use strict'

var co = require('co');
var thenify = require('thenify');
var exec = require('child_process').exec;

var ugrid = require('../../../ugrid/lib/ugrid-client.js')();

var args = JSON.parse(process.argv[2] || '{}');

var file = '/tmp/logreg244MB.data';
// var file = '/tmp/test.data';

var spark_home = process.env.SPARK_HOME;
var spark_host = process.env.SPARK_HOST;

co(function *() {
	var ui = yield ugrid.devices({type: 'webugrid'}, 0);

	// './bin/spark-submit --master spark://MacBook-Pro-de-Cedric.local:7077 /Users/cedricartigue/work/spark-1.3.0/examples/src/main/python/mllib/logistic_regression.py /tmp/test.data 1'

	// Generate data file in /tmp directory
	// console.log(__dirname + '/../../utils')

	var runUgrid = thenify(function (file, nIterations, callback) {
		var bin = __dirname + '/logreg-file.js';
		var cmd = '/usr/local/bin/node --harmony ' + bin + ' ' + file + ' ' + nIterations;
		var startTime = new Date();
		exec(cmd, function (err, stdout, stderr) {
			if (err) {
				ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {error: err.msg}});
				throw err;
			}
			var duration = Math.round(new Date() - startTime) / 1000;
			callback(null, duration);
		});
	});

	var runSpark = thenify(function (file, nIterations, callback) {
		// var spark_submit = '/Users/cedricartigue/work/spark-1.3.0/bin/spark-submit';
		var spark_submit = spark_home + '/bin/spark-submit';
		var bin = spark_home + '/examples/src/main/python/mllib/logistic_regression.py';
		var cmd = spark_submit + ' --master spark://' + spark_host + ':7077 ' + bin + ' ' + file + ' ' + nIterations;
		var startTime = new Date();
		exec(cmd, function (err, stdout, stderr) {
			if (err) {
				ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {error: err}});
				throw err;
			}
			var duration = Math.round(new Date() - startTime) / 1000;
			callback(null, duration);
		});
	});

	var time1 = [], time2 = [];
	var iterations = 1;
	// Spark first iteration
	var first_spark = yield runSpark(file, iterations);
	time2.push(first_spark);
	ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {chart: 'chart1', serie: 1, time: time2}});

	// Ugrid first iteration
	var first_ugrid = yield runUgrid(file, iterations);
	time1.push(first_ugrid);
	ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {chart: 'chart1', serie: 0, time: time1}});

	var iterations = 20;
	// Spark later iterations
	var duration = yield runSpark(file, iterations);
	var later = Math.round((duration - first_spark) / (iterations - 1) * 1000) / 1000;
	time2.push(later);
	ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {chart: 'chart1', serie: 1, time: time2}});

	// Ugrid later iterations
	var duration = yield runUgrid(file, iterations);
	var later = Math.round((duration - first_ugrid) / (iterations - 1) * 1000) / 1000;
	time1.push(later);
	ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {chart: 'chart1', serie: 0, time: time1}});

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});


