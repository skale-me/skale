#!/usr/bin/env node

var AWS = require('aws-sdk');
var MongoClient = require('mongodb').MongoClient;
var EJSON = require('mongodb-extended-json');
var Lines = require('../../lib/lines2.js');

var s3 = new AWS.S3({signatureVersion: 'v4'});

// s3://skale-demo/datasets/restaurants-ny.json is a publicly accessible
// mongo export database in a newline delimited json format.

var params = {Bucket: 'skale-demo', Key: 'datasets/restaurants-ny.json'};

//s3.getObject(params).createReadStream().pipe(process.stdout);

var mongoHost = 'localhost';
var mongoPort = 27017;
var dbAddr = 'mongodb://' + mongoHost + ':' + mongoPort + '/test';

MongoClient.connect(dbAddr, function (err, db) {
	if (err) throw err;

	var col = db.collection('restaurants');

	var stream = s3.getObject(params).createReadStream().pipe(new Lines());
	stream.on('data', function (lines, done) {
		console.log(lines.length);
		var a = lines.map(function (line) {
			return JSON.parse(line, function(k, v) {
				if (k == '$date') v = new Date(v);
				else if (v && v['$date']) v = v['$date']
				return v;
			})
		});
		col.insertMany(a, function (err, res) {
			if (err) {
				console.log(a)
				throw err
			}
			if (done)
				done();
		});
	})
	stream.on('end', function () {
		console.log('end')
		db.close();
	});
})
