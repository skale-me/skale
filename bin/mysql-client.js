#!/usr/local/bin/node --harmony

'use strict';

var mysql = require('mysql');
var connection = mysql.createConnection({
	host: '127.0.0.1',
	port: 3306,
	user: 'test',
	database: 'test',
	debug: true
});

connection.query('SELECT * FROM table', function(err, rows) {
	connection.end();
});
