/* ugrid client side library.  */
'use strict';

var net = require('net');
var thunkify = require('thunkify');
var ugridMsg = require('./ugrid-msg.js');
var websocket = require('websocket-stream');	// Keep this order to avoid browserify problems!!

var debug = false;

module.exports = UgridClient;

function UgridClient(arg) {
	if (!(this instanceof UgridClient))
		return new UgridClient(arg);
	var host = (arg && arg.host) || process.env.UGRID_HOST || 'localhost';
	var port = (arg && arg.port) || process.env.UGRID_PORT || 12346;
	var events = {};
	var hostId = {};
	var subscriptions = [];
	var sock;
	var saveMsg = true;
	var lastUuid, lastSent;
	var pending = {};
	var cid = 0;
	var cidMax = 10000000;
	var myId, myUuid, suid;
	var disconnect = false;
	var secondary;

	function connect_cb(callback) {
		sock = net.connect(port, host, function () {
			handleConnect(callback);
		});
		sock.setNoDelay(true);
		sock.on('end', events.end);
		sock.on('error', events.error);
	}

	function wsConnect_cb(callback) {
		sock = websocket('ws://' + host + ':' + port);
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
		send_cb(0, msg, function (err, res) {
			myId = res.id;
			myUuid = res.uuid;
			suid = myUuid.substr(0, 8);
			if (subscriptions.length > 0) {
				var publishers = [];
				for (var v in subscriptions)
					publishers.push({uuid: subscriptions[v]});
				setTimeout(function () {subscribe(publishers);}, 0);
			}
			callback(err, res);
		});
		decoder.on('Message', function (to, len, data) {
			var o = JSON.parse(data.slice(8));
			if (debug) console.error(suid + ' ' + myId + ' <--  ' + (o.from||0) + ' ' + (o.cid||0) + ' ' + o.cmd);
			if (o.ufrom && ! hostId[o.ufrom])
			 	hostId[o.ufrom] = o.from;
			if (o.cmd in events)
				events[o.cmd](o);
		});
	}

	function getId(uuid, nTry, o, callback) {
		send_cb(0, {cmd: 'id', data: uuid}, function (err, res) {
			if (res) {
				o.id = hostId[uuid] = res;
				o.from = myId;
				if (!o.cid) {
					o.cid = (cid == cidMax ? 0 : cid++);
					if (callback)
						pending[o.cid] = callback;
				}
				if (saveMsg) {
					lastUuid = uuid;
					lastSent = JSON.stringify(o);
				}
				sock.write(ugridMsg.encode(o));
				if (debug) console.error(suid + ' ' + myId + '  --> ' + (o.id || 0) + ' ' + o.cid + ': ' + o.cmd);
			} else {
				if (--nTry < 0) {
					if (pending[o.cid]) {
						console.error("getId failed");
						pending[o.cid]("getId failed");
						delete(pending[o.cid]);
					}
				} else {
					setTimeout(function () {
						getId(uuid, nTry, o, callback);
					}, Math.floor(Math.random() * 2000));
				}
			}
		});
	}

	function send_cb(uuid, o, callback) {
		if (uuid) {
			if (hostId[uuid])
				o.id = hostId[uuid];
			else
				return getId(uuid, 3, o, callback);
		}
		if (!o.cid) {
			o.cid = (cid == cidMax ? 0 : cid++);
			if (callback)
				pending[o.cid] = callback;
		}
		o.from = myId;
		if (saveMsg) { 
			lastUuid = uuid;
			lastSent = JSON.stringify(o);
		}
		sock.write(ugridMsg.encode(o));
		if (debug) console.error(suid + ' ' + myId + '  --> ' + (o.id || 0) + ' ' + o.cid + ': ' + o.cmd);
	}

	function subscribe(publishers) {
		var v;
		for (v in publishers) {
			if (subscriptions.indexOf(publishers[v].uuid) < 0)
				subscriptions.push(publishers[v].uuid);
		}
		send_cb(0, {cmd: 'subscribe', data: publishers});
	}

	function handleDisconnect(error) {
		if (disconnect) return;
		console.error(suid + ' ' + myId + ' Unexpected connection close ' + (error || ""));
		if (!secondary.host) process.exit(1);
		hostId = {};
		host = secondary.host;
		port = secondary.port;
		saveMsg = false;
		connect_cb(function () {
			console.error(suid + ' ' + myId + ' reconnected to ' + host + ':' + port);
			if (lastSent)
				send_cb(lastUuid, JSON.parse(lastSent));
			saveMsg = true;
		});
	}

	events.reply = function (o) {
		if (!pending[o.cid]) return;
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
		send_cb(0, {cmd: 'devices', data: o, id: 0}, function (err, res) {
			for (var i in res)
				hostId[res[i].uuid] = res[i].id;
			callback(err, res);
		});
	};

	this.publish = function (data) {
		send_cb(0, {cmd: 'publish', data: data, id: 2});
	};

	this.request_cb = function (dest, data, callback) {
		send_cb(dest.uuid, {cmd: 'request', ufrom: myUuid, data: data}, callback);
	};

	this.reply = function (msg, error, data) {
		var uuid = msg.ufrom;
		msg.cmd = 'reply';
		msg.id = msg.from;
		msg.ufrom = undefined;
		msg.data = data;
		msg.error = error;
		send_cb(uuid, msg);
	};

	this.subscribe = subscribe;

	this.unsubscribe = function unsubscribe(publishers) {
		var i, v;
		for (v in publishers) {
			if ((i = subscriptions.indexOf(publishers[v].uuid)) >= 0)
				subscriptions.splice(i, 1);
		}
		send_cb(0, {cmd: 'unsubscribe', data: publishers});
	};

	this.connect = thunkify(this.connect_cb);
	this.devices = thunkify(this.devices_cb);
	this.request = thunkify(this.request_cb);
	this.wsConnect = thunkify(this.wsConnect_cb);
}
