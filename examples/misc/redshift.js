#!/usr/bin/env node

var pg = require('pg');

//var endpoint = 'postgres://skale:LucaSAS16@skale-demo.cvqayf2p417l.eu-central-1.redshift.amazonaws.com:5439/dev';
var endpoint = 'postgres://skale:LucaSAS16@52.29.207.159:5439/dev';

var client = new pg.Client(endpoint);

console.log(client)

console.log('before connect')
client.connect(function (err) {
	if (err) return console.error('could not connect', err);
	console.log('connected!')
	client.end();
})
