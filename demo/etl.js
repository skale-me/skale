#!/usr/bin/env node
'use strict';

var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var url = 'mongodb://' + process.env.MONGO_HOST + ':' + process.env.MONGO_PORT + '/test';

MongoClient.connect(url, function (err, db) {
	if (err) console.log(err);
	console.log("Connected to " + url);

	var col = db.collection('restaurants');
	var col2 = db.collection('restaurants2');

	col2.drop(function (err) {
		col.count(function (err, res) {
			console.log("Processing " + res + " restaurants (without ugrid)");
			var max = res;
			var i = 0;
			var cursor = db.collection('restaurants').find({});
			cursor.forEach(function (doc) {
				doc.longitude = doc.address.coord[0];
				doc.latitude = doc.address.coord[1];
				doc.address.coord = undefined;
				col2.insertOne(doc, function (err) {
					if (err) console.log(err);
					if (++i == max) {
						console.log('finished');
						db.close();
					}
				});
			});
		});
	});
});
