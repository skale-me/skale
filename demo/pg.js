#!/usr/local/bin/node --harmony
'use strict';

var pg = require('pg');

var conString = "postgres://cedricartigue@localhost/cedricartigue";

/*
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);

CREATE TABLE clients (
    client_no integer,
    subscribed_on date,
    age numeric
);

INSERT INTO products VALUES (1, 'Cheese', 9.99);

*/

var N = 100;
var n = 0;

var client = new pg.Client(conString);
client.connect(function(err) {
    if(err) return console.error('could not connect to postgres', err);

    // client.query('SELECT NOW() AS "theTime"', function(err, result) {
    //     if (err) return console.error('error running query', err);
    //     console.log(result.rows[0].theTime);
    //     // output: Tue Jan 15 2013 19:12:47 GMT-600 (CST)
    //     client.end();
    // });

    function insertRandomClient(done) {
        var client_no = n++;
        var subscribed_on = new Date();
        var age = Math.round(Math.random() * 60) + 20;
        client.query('INSERT INTO clients VALUES ($1, $2, $3)', [client_no, subscribed_on, age], function(err, result) {
            if (err) return console.error('error running query', err);
            if (n == N) {done();}
            else insertRandomClient(done);
        });
    }

    insertRandomClient(function() {
        console.log('Successfully inserted ' + N + ' random clients');
        client.end();
    })

    // client.query('INSERT INTO visit VALUES ($1)', [new Date()], function(err, result) {
    //     if (err) return console.error('error running query', err);        
    //     console.log(result);
    //     client.end();
    // });
});
