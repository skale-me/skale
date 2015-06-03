#!/usr/local/bin/node --harmony

var MongoClient = require('mongodb').MongoClient, assert = require('assert');

// Connection URL
var url = 'mongodb://pc2:27017/ugrid';
// Use connect method to connect to the Server
MongoClient.connect(url, function(err, db) {
	assert.equal(null, err);
	console.log("Connected correctly to server");

	var col = db.collection('bigdata');
	var cursor = col.find({}, {_id: false});
	
	cursor.on('data', function(doc) {
		console.dir(doc);
	});

	cursor.once('end', function() {
		db.close();
	});
});
