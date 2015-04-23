#!/usr/local/bin/node --harmony

'use strict';

var mysql = require('mysql2');
var connection = mysql.createConnection({
	host: '127.0.0.1',
	port: 3307,
	user: 'cedric', 
	database: 'test',
	debug: true
});
 
connection.query('SHOW DATABASES', function(err, rows) {
	connection.end();
});
