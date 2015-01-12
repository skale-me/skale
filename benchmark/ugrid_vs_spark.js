#!/usr/local/bin/node --harmony

// this programme checks every second the server 
// to know if all workers have been connected to the server
// then we run all the unit tests
var co = require('co');
var thunkify = require('thunkify');
var fs = require('fs');
var spawn = require('child_process').spawn;
var UgridClient = require('../lib/ugrid-client.js');

var MAX_ITERATION = 100;
var M = 1;
var nWorkers = 2;
var file = "/tmp/data.txt"

var grid = new UgridClient({
	host: 'localhost',
	port: 12346
});

co(function*(){
	var res = yield grid.connect();
	var N = process.argv[2] || nWorkers;
	// Query devices, get uuid array
	var t0 = setInterval(function() {
		grid.devices_cb({type: 'worker'}, function (err, res) {
			if (err) 
				throw err;
			if (res.length != N)
				throw 'Not enough workers to run tests';
			else {
				clearInterval(t0);
				runTests(grid);
			}
		});
	}, 1000);
})();

function runTests(grid) {
	var first_iteration_time_ugrid = [];
	var first_iteration_time_spark = [];
	var later_iterations_time_ugrid = [];
	var later_iterations_time_spark = [];	
	function logregTest_cb(cmd, arg, time, callback) {
		var startTime = new Date();
		var prog = spawn(cmd, arg); // run in a asynchronous way log reg ugrid
		var resultTosend;
		prog.stdout.on('data', function (result) {
			resultTosend = result; //last console.log in this case final w  
		});

		prog.stderr.on('data', function (result) {});

		prog.on('exit', function (code) {
			time.push((new Date() - startTime) / 1000);
			callback(null, String(resultTosend)); //return the result
		});
	}
	var logregTest = thunkify(logregTest_cb);

	co(function*() {
		// Compute average first iteration time
		var ITERATION = 1;
		for (var it = 0; it < M; it++) {
			//~ console.log('Run ugrid logistic regression');
			cmd = 'node';
			arg = ['--harmony', 'logreg-textFile.js', String(file), String(ITERATION)]
			var res_ugrid = yield logregTest(cmd, arg, first_iteration_time_ugrid); 

			//~ console.log('Run spark logistic regression');
			cmd = '../../../spark/spark-1.1.1-bin-hadoop1/bin/spark-submit';
			arg = ['--master', 'spark://192.168.1.29:7077', '../../../spark/spark-1.1.1-bin-hadoop1/examples/src/main/python/mllib/logistic_regression_optimise.py', String(file), String(ITERATION)]

			var res_spark = yield logregTest(cmd, arg, first_iteration_time_spark);
			//~ console.log(res_spark);

			// Evaluate MSE between ugrid and spark computed weights
			var r1 = res_ugrid.split(" ").map(parseFloat);
			var r2 = res_spark.split(" ").map(parseFloat);
			var D = r1.length;
			var mse = 0;
			for (var i = 0; i < D; i++)
				mse += Math.pow(r1[i] - r2[i], 2);
			mse /= D;
			console.log('mse : ' + mse);
			//~ console.log(it + ':ugrid time (s) : ' + first_iteration_time_ugrid[it]);
			//~ console.log(it + ':spark time (s) : ' + first_iteration_time_spark[it]);
		}
		
		t0_ugrid = first_iteration_time_ugrid.reduce(function(a, b) {return a + b}) / M;
		t0_spark = first_iteration_time_spark.reduce(function(a, b) {return a + b}) / M;

		console.log('Iterations : ' + ITERATION)
		console.log('Ugrid average time (s) : ', t0_ugrid)
		console.log('Spark average time (s) : ', t0_spark)

		// Compute average first iteration time
		var ITERATION = MAX_ITERATION;
		for (var it = 0; it < M; it++) {
			//~ console.log('Run ugrid logistic regression');
			cmd = 'node';
			arg = ['--harmony', 'logreg-textFile.js', String(file), String(ITERATION)]
			var res_ugrid = yield logregTest(cmd, arg, later_iterations_time_ugrid); 

			//~ console.log('Run spark logistic regression');
			cmd = '../../../spark/spark-1.1.1-bin-hadoop1/bin/spark-submit';
			arg = ['--master', 'spark://192.168.1.29:7077', '../../../spark/spark-1.1.1-bin-hadoop1/examples/src/main/python/mllib/logistic_regression_optimise.py', String(file), String(ITERATION)]

			var res_spark = yield logregTest(cmd, arg, later_iterations_time_spark);

			// Evaluate MSE between ugrid and spark computed weights
			var r1 = res_ugrid.split(" ").map(parseFloat);
			var r2 = res_spark.split(" ").map(parseFloat);
			var D = r1.length;
			var mse = 0;
			for (var i = 0; i < D; i++)
				mse += Math.pow(r1[i] - r2[i], 2);
			mse /= D;
			console.log('mse : ' + mse);
			//~ console.log(it + ':ugrid time (s) : ' + later_iterations_time_ugrid[it]);
			//~ console.log(it + ':spark time (s) : ' + later_iterations_time_spark[it]);
		}
		
		t1_ugrid = later_iterations_time_ugrid.reduce(function(a, b) {return a + b}) / M;
		t1_spark = later_iterations_time_spark.reduce(function(a, b) {return a + b}) / M;

		console.log('Iterations : ' + ITERATION)
		console.log('Ugrid average time (s) : ', t1_ugrid)
		console.log('Spark average time (s) : ', t1_spark)

		//~ tother_ugrid = (t1_ugrid - t0_ugrid) / (MAX_ITERATION - 1);
		//~ tother_spark = (t1_spark - t0_spark) / (MAX_ITERATION - 1);
//~ 
		//~ console.log('Ugrid later iterations average time (s) : ', tother_ugrid)
		//~ console.log('Spark later iterations average time (s) : ', tother_spark)
		grid.disconnect();
	})();
}	
