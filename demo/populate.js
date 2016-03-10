#!/usr/bin/env node

const child_process = require('child_process');
const http = require('http');

const mongoimport = child_process.spawn('mongoimport', [
	'--host=' + process.env.MONGO_HOST,
	'--port=' + process.env.MONGO_PORT,
	'--drop',
	'--db=test',
	'--collection=restaurants'
]);

const req = http.request({
	hostname: 'skale-demo.s3.amazonaws.com',
	path: '/datasets/restaurants-ny.json'
}, function (stream) {
	stream.pipe(mongoimport.stdin);
	stream.on('end', function () {console.log('import completed');});
});
req.end();
