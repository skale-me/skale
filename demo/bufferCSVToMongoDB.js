#!/usr/local/bin/node --harmony
'use strict';

/*
	CSV file contains stripe events gathered from stripe API,
	this programm parse raw CSV and populate a mongoDB collection
*/

var parseCSV = require('papaparse').parse;
var fs = require('fs');
var readline = require('readline');
var MongoClient = require('mongodb').MongoClient;

// var targetDatbase = 'mongodb://localhost:27017/ugrid';

var targetDatbase = 'mongodb://dev.ugrid.net:27017/ugrid';

// Connect to mongoDB
MongoClient.connect(targetDatbase, function(err, db) {
	var rl = readline.createInterface({
		input: fs.createReadStream('/Users/cedricartigue/Desktop/stripe_events_bak.csv', {encoding: 'utf8'})
	});

	var first_line = true, fields, lineCount = 0;
	var customers = {};

	rl.on('line', function(line) {
		lineCount++;
		if (first_line) {
			first_line = false;
			fields = line.split(',').map(JSON.parse);
			return;
		};
		// rl.pause();
		var data = parseCSV(line, {dynamicTyping: true});
		var json = {};
		for (var i = 0; i < fields.length; i++)
			json[fields[i]] = data.data[0][i];
		json.value = JSON.parse(json.value);

		db.collection('buffer').insert(json, function(err, result) {
			// if (err) 
			// rl.resume();
			// db.close();
			// process.exit();
		});
	});

	rl.on('close', function() {
		db.close();
	})

	// var cursor = db.collection('connexions').find({}, {_id: false});
	// cursor.on('data', function(data) {
	// 	console.log(data);
	// 	db.close();
	// })
});
