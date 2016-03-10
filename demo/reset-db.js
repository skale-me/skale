#!/usr/bin/env node
'use strict';

var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var url = 'mongodb://' + process.env.MONGO_HOST + ':' + process.env.MONGO_PORT + '/test';

MongoClient.connect(url, function (err, db) {
	if (err) console.log(err);
	console.log("Connected to " + url);
	db.dropDatabase(function(err, res) {
		console.log("database dropped");
		db.close();
	});
});
