#!/usr/bin/env node

var AWS = require('aws-sdk');

var s3 = new AWS.S3({signatureVersion: 'v4'});

// s3://skale-demo/datasets/restaurants-ny.json is a publicly accessible
// mongo export database in a newline delimited json format.

var params = {Bucket: 'skale-demo', Key: 'datasets/restaurants-ny.json'};

s3.getObject(params).createReadStream().pipe(process.stdout);
