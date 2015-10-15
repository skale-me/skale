#!/usr/bin/env node

'use strict';

var ugrid = require('ugrid');
var MongoClient = require('mongodb').MongoClient;

var monhost = process.env.MONGO_PORT_27017_TCP_ADDR;
var monport = process.env.MONGO_PORT_27017_TCP_PORT;
var monurl = 'mongodb://' + monhost + ':' + monport + '/test';

ugrid.context(function (err, uc) {
	if (err) throw err;
	console.log('connected to ugrid');

	MongoClient.connect(monurl, function (err, db) {
		if (err) throw err;
		console.log('connected to Mongo')
		uc.end();
	});
});
