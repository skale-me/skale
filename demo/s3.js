#!/usr/local/bin/node --harmony
'use strict';

var AWS = require('aws-sdk');
var Lines = require('../lib/lines.js');
var ugrid = require('ugrid');

/* ====================================================================== */
//	Create a bucket, and put an object inside it
/* ====================================================================== */
// var s3 = new AWS.S3({params: {Bucket: 'ugrid_cedric', Key: 'helloworld'}});
// s3.createBucket(function(err) {
// 	if (err) console.log("Error:", err);
// 	else {
// 		s3.upload({Body: 'Hello world\nHello cedric'}, function() {
// 			console.log("Successfully uploaded data to myBucket/myKey");
// 		});
// 	}
// });

/* ====================================================================== */
//	List all buckets
/* ====================================================================== */
// var s3 = new AWS.S3();
// s3.listBuckets(function(err, data) {
// 	if (err) {console.log("Error:", err);}
// 	else {
// 		for (var index in data.Buckets) {
// 			var bucket = data.Buckets[index];
// 			console.log("Bucket: ", bucket.Name, ' : ', bucket.CreationDate);
// 		}
// 	}
// });

/* ====================================================================== */
//	List keys inside given bucket
/* ====================================================================== */
// var s3 = new AWS.S3();
// var params = {
//   Bucket: 'ugrid_cedric'
// };

// s3.listObjects(params, function(err, data) {
// 	if (err) console.log(err, err.stack); // an error occurred
// 	else console.log(data);           // successful response
// });

/* ====================================================================== */
//	get data from text File bucket
/* ====================================================================== */
// var s3 = new AWS.S3();

// var lines = new Lines();

// lines.on('data', function(data) {
// 	console.log('time')
// 	console.log(data);
// })

// s3.getObject({Bucket: 'ugrid_cedric', Key: 'helloworld'})
// 	.createReadStream()
// 	.pipe(lines);

/* ====================================================================== */
//	Inject content of bucket key inside ugrid cluster
/* ====================================================================== */
ugrid.context(function(err, uc) {
	console.log('Connected to ugrid');

	var s3 = new AWS.S3();
	var stream = s3.getObject({Bucket: 'ugrid_cedric', Key: 'helloworld'}).createReadStream();

	uc.lineStream(stream).count(function(err, res) {
		console.log(err)
		console.log(res)
		console.log('bucket file contains %d lines ', res);
		uc.end();
	})
});


