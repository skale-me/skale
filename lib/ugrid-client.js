/*
  ugrid client side library.
*/
'use strict';

var net = require('net');
var thunkify = require('thunkify');
var ugridMsg = require('./ugrid-msg.js');

module.exports = UgridClient;

function UgridClient(arg) {
	if (!(this instanceof UgridClient))
		return new UgridClient(arg);
	var events = {};
	var sock;
	var pending = {};
	var cid = 0;
	var cidMax = 10000000;
	var myId;
	var disconnect = false;
	var secondary;

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
		console.error('Unexpected connection close');
		process.exit(1);
	};

	events.error = function (error) {
		if (disconnect) return;
		console.error('IO: Connection ' + error);
		process.exit(1);
	};

	events.broadcast = function (o) {
		if ('secondary' in o.data)
			secondary = o.data.secondary;
	};

	this.connect_cb = function (callback) {
		sock = net.connect(arg.port, arg.host, function () {
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
		});
		sock.on('end', events.end);
		sock.on('error', events.error);
	};

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
	
	this.devices_cb = function (o, callback) {
		send_cb({cmd: 'devices', data: o}, callback);
	};

	this.request_cb = function (dest, data, callback) {
		send_cb({cmd: 'request', id: dest.id, data: data}, callback);
	};

	this.reply_cb = function (msg, error, data, callback) {
		msg.cmd = 'reply';
		msg.id = msg.from;
		msg.data = data;
		msg.error = error;
		send_cb(msg, callback);
	};

	this.connect = thunkify(this.connect_cb);
	this.devices = thunkify(this.devices_cb);
	this.request = thunkify(this.request_cb);
	this.reply = thunkify(this.reply_cb);
	this.send = thunkify(this.send_cb);
};
