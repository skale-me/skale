#!/usr/local/bin/node --harmony

var co = require('co');
var thunkify = require('thunkify');
var request = require('request');

var get = thunkify(request.get);
var put = thunkify(request.put);
var post = thunkify(request.post);
var del = thunkify(request.del);
var url = 'http://localhost:4730';

co(function *(){
	var device, req, auth;

	console.log('# register');	
	console.log('=> curl -X POST -d "type=drone&color=black" http://localhost:4730/devices');
	req = yield post({uri: url + '/devices', form: {type: 'drone', color: 'black'}});
	console.log(req[1] + '\n');
	device = JSON.parse(req[1]);
	auth = ' --header "auth_uuid: ' + device.uuid + '" --header "auth_token: ' + device.token + '"';

	console.log('# get');
	console.log('=> curl -X GET http://localhost:4730/devices/' + device.uuid + auth);
	req = yield get({uri: url + '/devices/' + device.uuid, headers: {auth_uuid: device.uuid, auth_token: device.token}});
	console.log(req[1] + '\n');

	console.log('# set');
	console.log('=> curl -X PUT -d "color=blue" http://localhost:4730/devices/' + device.uuid + auth);
	req = yield put({uri: url + '/devices/' + device.uuid, form: {color: 'blue'}, headers: {auth_uuid: device.uuid, auth_token: device.token}});
	console.log(req[1] + '\n');

	console.log('# query');
	console.log('=> curl -X GET http://localhost:4730/devices?type=drone&color=blue' + auth);
	req = yield get({uri: url + '/devices', qs: {type: 'drone', color: 'blue'}, headers: {auth_uuid: device.uuid, auth_token: device.token}});
	console.log(req[1] + '\n');

	console.log('# unregister');
	console.log('=> curl -X DELETE http://localhost:4730/devices/' + device.uuid + auth);
	req = yield del({uri: url + '/devices/' + device.uuid, headers: {auth_uuid: device.uuid, auth_token: device.token}});
	console.log(req[1] + '\n');

	console.log('# list all');
	console.log('=> curl -X GET http://localhost:4730');
	req = yield get({uri: url});
	console.log(req[1] + '\n');		
})()
