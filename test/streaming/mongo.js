#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');
var MongoClient = require('mongodb').MongoClient; 
var assert = require('assert');

co(function *() {
	var uc = yield ugrid.context();
	var url = 'mongodb://pc2:27017/ugrid';

	MongoClient.connect(url, function(err, db) {
		assert.equal(null, err);

		var col = db.collection('bigdata');
		var cursor = col.find({}, {_id: false});

		var out = uc.objectStream(cursor, {N: 4}).collect({stream: true});

		out.on('data', function(res) {
			console.dir(res);
		});

		out.on('end', function(res) {
			db.close();
			uc.end();
		});
	});
}).catch(ugrid.onError);



