/*
  ugrid client side library.
*/
'use strict';

var net = require('net');
var thunkify = require('thunkify');
var ugridMsg = require('./ugrid-msg.js');
var websocket = require('websocket-stream');	// Keep this order to avoid browserify problems!!

module.exports = UgridClient;

function UgridClient(arg) {
	if (!(this instanceof UgridClient))
		return new UgridClient(arg);
	if (!arg.host) arg.host = 'localhost';
	if (!arg.port) arg.port = 12346;
	var events = {};
	var sock;
	var pending = {};
	var cid = 0;
	var cidMax = 10000000;
	var myId;
	var disconnect = false;
	var secondary;
	var hosts, nhosts;

	function connect_cb(callback) {
		sock = net.connect(arg.port, arg.host, function () {
			handleConnect(callback);
		});
		sock.on('end', events.end);
		sock.on('error', events.error);
	}

	function wsConnect_cb(callback) {
		sock = websocket('ws://' + arg.host + ':' + arg.port);
		handleConnect(callback);
		sock.on('end', events.end);
		sock.on('error', events.error);
	}

	function handleConnect(callback) {
		var decoder = ugridMsg.Decoder();
		sock.pipe(decoder);

		send_cb({cmd: 'connect', data: arg.data}, function (err, res) {
			myId = res.id;
			callback(err, res);
		});

		decoder.on('Message', function (to, len, data) {
			var o = JSON.parse(data.slice(8));
			if (o.cmd in events)
				events[o.cmd](o);
		});
	}

	function send_cb(o, callback) {
		if (!o.cid) {
			o.cid = (cid == cidMax ? 0 : cid++);
			if (callback)
				pending[o.cid] = callback;
		}
		o.from = myId;
		sock.write(ugridMsg.encode(o));
	}

	events.reply = function (o) {
		pending[o.cid](o.error, o.data);
		delete(pending[o.cid]);
	};

	events.end = function () {
		if (disconnect) return;
		console.error('Unexpected connection close');
		if (!secondary.host) process.exit(1);
		console.log('reconnect to secondary');
		console.log(secondary);
		arg.host = secondary.host;
		arg.port = secondary.port;
		connect_cb(function () {
			console.log('reconnected');
		});
	};

	events.error = function (error) {
		if (disconnect) return;
		console.error('IO: Connection ' + error);
		process.exit(1);
	};

	events.secondary = function (o) {
		secondary = o.data;
	};

	this.connect_cb = connect_cb;
	this.wsConnect_cb = wsConnect_cb;

	this.on = function (event, callback) {
		if (event === 'end' || event === 'error') {
			sock.removeListener('end', events[event]);
			sock.on(event, callback);
		}
		events[event] = callback;
	};

	this.disconnect = function () {
		disconnect = true;
		sock.end();
	};
	
	this.send_cb = send_cb;

	this.send = function (o) {send_cb(o);};

	this.devices_cb = function (o, callback) {
		send_cb({cmd: 'devices', data: o, id: 0}, function (err, res) {
		//	for (var i in res) {
		//		hosts[res[i].uuid] = 
		//	}
			callback(err, res);
		});
	};

	this.publish = function (data) {
		send_cb({cmd: 'publish', data: data, id: 2});
	};

	this.request_cb = function (dest, data, callback) {
		send_cb({cmd: 'request', id: dest.id, data: data}, callback);
	};

	this.reply = function (msg, error, data) {
		msg.cmd = 'reply';
		msg.id = msg.from;
		msg.data = data;
		msg.error = error;
		send_cb(msg);
	};

	this.subscribe = function (publishers) {
		send_cb({cmd: 'subscribe', data: publishers});
	};

	this.unsubscribe = function (publishers) {
		send_cb({cmd: 'unsubscribe', data: publishers});
	};

	this.connect = thunkify(this.connect_cb);
	this.devices = thunkify(this.devices_cb);
	this.request = thunkify(this.request_cb);
	this.wsConnect = thunkify(this.wsConnect_cb);
};
