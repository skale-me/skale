/* ugrid client side library.  */
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
	var prevUuid = [], hostUuid = [];
	var subscriptions = [];
	var sock;
	var pending = {};
	var cid = 0;
	var cidMax = 10000000;
	var myId, myUuid;
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

		var msg = {cmd: 'connect', data: arg.data};
		if (myUuid)
			msg.uuid = myUuid;
		send_cb(msg, function (err, res) {
			myId = res.id;
			myUuid = res.uuid;
			if (subscriptions.length > 0) {
				var publishers = [];
				for (var v in subscriptions)
					publishers.push({uuid: subscriptions[v]});
				setTimeout(function () {subscribe(publishers)}, 0);
			}
			callback(err, res);
		});
		decoder.on('Message', function (to, len, data) {
			var o = JSON.parse(data.slice(8));
			// process.stdout.write('<------------');
			// console.log(o);
			if (o.ufrom) {
				hostUuid[o.from] = o.ufrom;
			}
			if (o.cmd in events)
				events[o.cmd](o);
		});
	}

    function retryId(id, nTry, o) {
		send_cb({cmd: 'id', data: id}, function (err, res) {
			var r;
			if (!res) {
				if (--nTry < 0) {
					if (pending[o.cid]) {
						pending[o.cid]("retryId failed");
						delete(pending[o.cid]);
					}
				} else
					setTimeout(function () {
						retryId(id, nTry, o);
					}, Math.floor(Math.random() * 2000));
			} else {
				hostUuid[res] = prevUuid[o.id];
				o.id = res;
				o.from = myId;
				r = sock.write(ugridMsg.encode(o));
				// process.stdout.write(r + ' ------------> ');
				// console.log(o);
			}
		});
	}

	function send_cb(o, callback) {
		var r;
		if (!o.cid) {
			o.cid = (cid == cidMax ? 0 : cid++);
			if (callback)
				pending[o.cid] = callback;
		}
		if (o.id > 3 && !hostUuid[o.id]) {
			retryId(prevUuid[o.id], 3, o);
		} else {
			o.from = myId;
			r = sock.write(ugridMsg.encode(o));
			// process.stdout.write(r + ' ------------> ');
			// console.log(o);
		}
	}

	function subscribe(publishers) {
		var v;
		for (v in publishers) {
			if (subscriptions.indexOf(publishers[v].uuid) < 0)
				subscriptions.push(publishers[v].uuid);
		}
		send_cb({cmd: 'subscribe', data: publishers});
	}

	function handleDisconnect(error) {
		if (disconnect) return;
		console.error('Unexpected connection close ' + (error || ""));
		if (!secondary.host) process.exit(1);
		for (var i in hostUuid) {
			prevUuid[i] = hostUuid[i];
			hostUuid[i] = undefined;
		}
		arg.host = secondary.host;
		arg.port = secondary.port;
		connect_cb(function () {
			console.log('reconnected to ' + arg.host + ':' + arg.port);
		});
	}

	events.reply = function (o) {
		pending[o.cid](o.error, o.data);
		delete(pending[o.cid]);
	};

	events.end = events.error = handleDisconnect;

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
			for (var i in res)
				hostUuid[res[i].id] = res[i].uuid;
			callback(err, res);
		});
	};

	this.publish = function (data) {
		send_cb({cmd: 'publish', data: data, id: 2});
	};

	this.request_cb = function (dest, data, callback) {
		send_cb({cmd: 'request', id: dest.id, ufrom: myUuid, data: data}, callback);
	};

	this.reply = function (msg, error, data) {
		msg.cmd = 'reply';
		msg.id = msg.from;
		msg.ufrom = undefined;
		msg.data = data;
		msg.error = error;
		send_cb(msg);
	};

	this.subscribe = subscribe;

	this.unsubscribe = function unsubscribe(publishers) {
		var i, v;
		for (v in publishers) {
			if ((i = subscriptions.indexOf(publishers[v].uuid)) >= 0)
				subscriptions.splice(i, 1);
		}
		send_cb({cmd: 'unsubscribe', data: publishers});
	};

	this.connect = thunkify(this.connect_cb);
	this.devices = thunkify(this.devices_cb);
	this.request = thunkify(this.request_cb);
	this.wsConnect = thunkify(this.wsConnect_cb);
}
