#!/usr/local/bin/node --harmony

'use strict';

var mysql = require('mysql2');
var server = mysql.createServer();

server.listen(3307);
server.on('connection', function(conn) {
    console.log('New client connected to mysql server');

    conn.serverHandshake({
        protocolVersion: 10,
        serverVersion: 'node.js rocks',
        connectionId: 1234,
        statusFlags: 2,
        characterSet: 8,
        capabilityFlags: 0xffffff
    });

    conn.on('field_list', function(table, fields) {
        console.log('field list:', table, fields);
        conn.writeEof();
    });

    var remote = mysql.createConnection({user: 'cedric', database: 'test', host:'127.0.0.1', password: ''});

    conn.on('query', function(sql) {
        remote.query(sql, function(err) { // overloaded args, either (err, result :object)
            // or (err, rows :array, columns :array)
            if (Array.isArray(arguments[1])) {
                // response to a 'select', 'show' or similar
                var rows = arguments[1], columns = arguments[2];
                console.log('rows', rows);
                console.log('columns', columns);
                conn.writeTextResult(rows, columns);
            } else {
                // response to an 'insert', 'update' or 'delete'
                var result = arguments[1];
                console.log('result', result);
                conn.writeOk(result);
            }
        });
        // var rows = [ { Database: 'information_schema' }, { Database: 'test' } ]
        // var columns = [ { catalog: 'def',
        //     schema: 'information_schema',
        //     name: 'Database',
        //     orgName: 'SCHEMA_NAME',
        //     table: 'SCHEMATA',
        //     orgTable: 'SCHEMATA',
        //     characterSet: 33,
        //     columnLength: 192,
        //     columnType: 253,
        //     flags: 1,
        //     decimals: 0 } ]
        // conn.writeTextResult(rows, columns);
    });

    conn.on('end', function() {});
});
