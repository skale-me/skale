/* ugrid client side library */

// Todo:
// - use eventEmitter class

'use strict';

var net = require('net');
var util = require('util');
var stream = require('stream');
var thunkify = require('thunkify');
var websocket = require('websocket-stream');    // Keep this order for browserify

var minMulticast = 4294901760;	 // 2^32 - 2^16, 65536 available multicast ids

function encode(msg) {
	var str = JSON.stringify(msg),
	    len = Buffer.byteLength(str),
		buf = new Buffer(len + 8);
	buf.writeUInt32LE(msg.id, 0, true);
	buf.writeUInt32LE(len, 4, true);
	buf.write(str, 8);
	return buf;
}

function ToGrid(opt) {
	if (!(this instanceof ToGrid))
		return new ToGrid(opt);
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(ToGrid, stream.Transform);

ToGrid.prototype._transform = function (msg, encoding, done) {
	done(null, encode(msg));
};

function FromGrid() {
	if (!(this instanceof FromGrid))
		return new FromGrid();
	stream.Transform.call(this, {objectMode: true});
	this._buf = null;
}
util.inherits(FromGrid, stream.Transform);

FromGrid.prototype._transform = function (chunk, encoding, done) {
	var len, data, buf, offset = 0;

	if (this._buf) {
		chunk = Buffer.concat([this._buf, chunk], this._buf.length + chunk.length);
		this._buf = null;
	}
	do {
		buf = chunk.slice(offset);
		if (buf.length < 8) {
			this._buf = buf;
			break;
		}
		len = buf.readUInt32LE(4, true);
		if (buf.length < 8 + len) {
			this._buf = buf;
			break;
		}
		data = buf.slice(0, 8 + len);
		this.push(data);
		offset += 8 + len;
	} while (offset < chunk.length);
	done();
};

function Consumer(opt) {
	if (!(this instanceof Consumer))
		return new Consumer(opt);
	stream.Transform.call(this, {objectMode: true});
	this.subscriber = {};
}
util.inherits(Consumer, stream.Transform);

Consumer.prototype._transform = function (chunk, encoding, done) {
	try {
		var msg = JSON.parse(chunk.slice(8));

		if (msg.ufrom && !this.client.hostId[msg.ufrom])
			this.client.hostId[msg.ufrom] = msg.from;

		if (msg.cmd == 'reply') {
			if (this.client.pending[msg.cid]) {
				this.client.pending[msg.cid](msg.error, msg.data);
				delete this.client.pending[msg.cid];
			} else {
				console.warn('[' + this.client.id +  '] unwanted reply: ' + util.inspect(msg));
			}
			done();
		} else if (this.subscriber[msg.cmd]) {
			this.subscriber[msg.cmd].write(JSON.stringify(msg.data), done);
		} else if (this.client.event[msg.cmd]) {
			this.client.event[msg.cmd](msg);
			done();
		} else done();
	} catch (error) {
		console.error(error);
		throw error;
	}
};

function PubStream(client, name, dest) {
	if (!(this instanceof PubStream))
		return new PubStream(client, name, dest);
	stream.Transform.call(this, {objectMode: true});
	this.dest = dest;
	this.client = client;
	this.cmd = name;
	this.pipe(client.output);
}
util.inherits(PubStream, stream.Transform);

PubStream.prototype._transform = function (chunk, encoding, done) {
	if (this.id === undefined) {
		var self = this;
		if (self.dest) {
			this.client.send_cb(0, {cmd: 'id', data: this.dest.uuid}, function (err, res) {
				if (err || res === undefined)
					throw new Error("PubStream error: " + err);
				self.client.hostId[self.dest.uuid] = self.id = res;
				done(null, {cmd: self.cmd, id: self.id, data: chunk.toString()});
			});
		} else {
			this.client.send_cb(0, {cmd: 'tid', data: this.cmd}, function (err, res) {
				if (err || res === undefined)
					throw new Error("PubStream error: " + err);
				self.client.topicId[self.cmd] = self.id = minMulticast + res;
				done(null, {cmd: self.cmd, id: self.id, data: chunk.toString()});
			});
		}
	} else
		done(null, {cmd: this.cmd, id: this.id, data: chunk.toString()});
};

function Client(opt, callback) {
	if (!(this instanceof Client))
		return new Client(opt, callback);
	var inBrowser = (typeof window != 'undefined');
	opt = opt || {};
	if (!opt.ws) opt.ws = inBrowser ? true : process.env.UGRID_WS;
	if (!opt.host && !inBrowser) opt.host = process.env.UGRID_HOST;
	if (!opt.port && !inBrowser) opt.port = process.env.UGRID_PORT;
	opt.host = opt.host || 'localhost';
	opt.port = opt.port || (opt.ws ? 12348 : 12346);
	var self = this;
	this.pending = {};
	this.event = {};
	this.hostId = {};
	this.topicId = {};
	this.cid = 0;
	if (opt.ws) {
		this.sock = websocket('ws://' + opt.host + ':' + opt.port);
	} else {
		this.sock = net.connect(opt.port, opt.host);
		this.sock.setNoDelay();
	}
	this.input = new FromGrid();
	this.output = new ToGrid();
	this.consumer = new Consumer();
	this.consumer.client = this;
	this.sock.pipe(this.input).pipe(this.consumer);
	this.output.pipe(this.sock);
	this.send_cb(0, {cmd: 'connect', data: opt.data}, function (err, data) {
		if (data) {
			self.id = data.id;
			self.uuid = data.uuid;
			if ('connect' in self.event) self.event.connect();
		}
		if (callback) callback(err, data);
	});
	this.sock.on('end', function () {
		if (self.event.close) self.event.close();
		else if (!inBrowser) process.exit(0);
	});
	this.sock.on('close', function () {
		if (self.event.close) self.event.close();
		else if (!inBrowser) process.exit(0);
	});
	this.sock.on('error', function (err) {
		if (self.event.error) self.event.error(err);
		else if (!inBrowser) process.exit(0);
	});
}

Client.prototype._getId = function (uuid, nTry, msg, callback) {
	var self = this;
	this.send_cb(0, {cmd: 'id', data: uuid}, function (err, res) {
		if (res) {
			msg.id = self.hostId[uuid] = res;
			msg.from = self.id;
			self.output.write(msg);
		} else {
			if (--nTry < 0) {
				if (self.pending[msg.cid]) {
					console.error("_getId failed");
					self.pending[msg.cid]("_getIDd failed");
					delete self.pending[msg.cid];
				}
			} else {
				setTimeout(function () {
					self._getId(uuid, nTry, msg, callback);
				}, Math.floor(Math.random() * 2000));
			}
		}
	});
};

Client.prototype.send_cb = function (uuid, msg, callback) {
	msg.cid = this.cid++;
	this.pending[msg.cid] = callback;
	msg.from = this.id;
	if (uuid) {
		if (this.hostId[uuid]) msg.id = this.hostId[uuid];
		else return this._getId(uuid, 3, msg, callback);
	}
	this.output.write(msg);
};

Client.prototype.devices_cb = function (o, callback) {
	var self = this;
	this.send_cb(0, {cmd: 'devices', data: o}, function (err, data) {
		for (var i in data)
			self.hostId[data[i].uuid] = data[i].id;
		callback(err, data);
	});
};

Client.prototype.get_cb = function (uuid, callback) {
	this.send_cb(0, {cmd: 'get', data: uuid}, callback);
};

Client.prototype.subscribe = function (topic) {
	this.output.write({cmd: 'subscribe', data: topic});
	return this;
};

Client.prototype.unsubscribe = function (topic) {
	this.output.write({cmd: 'unsubscribe', data: topic});
};

Client.prototype.publish = function (topic, content) {
	if (!(topic in this.topicId)) {
		var self = this;
		this.send_cb(0, {cmd: 'tid', data: topic}, function (err, res) {
			if (err || res === undefined) return;
			self.topicId[topic] = res;
			self.output.write({cmd: topic, id: minMulticast + res, data: content});
		});
	} else
		this.output.write({cmd: topic, id: minMulticast + this.topicId[topic], data: content});
};

Client.prototype.request_cb = function (dest, data, callback) {
	this.send_cb(dest.uuid, {cmd: 'request', ufrom: this.uuid, data: data}, callback);
};

Client.prototype.reply = function (msg, error, data) {
	console.assert(msg.cmd === 'request');
	msg.cmd = 'reply';
	msg.id = msg.from;
	msg.ufrom = null;
	msg.data = data;
	msg.error = error;
	this.output.write(msg);
};

Client.prototype.set = function (data) {
	this.output.write({cmd: 'set', data: data});
};

Client.prototype.pipe = function (topic, stream) {
	this.consumer.subscriber[topic] = stream;
	return stream;
};

Client.prototype.on = function (ev, callback) {
	this.event[ev] = callback;
	return this;
};

Client.prototype.createWriteStream = function (name, dest) {
	return new PubStream(this, name, dest);
};

Client.prototype.end = function () {
	this.output.end({cmd: 'end'});
};

Client.prototype.devices = thunkify(Client.prototype.devices_cb);
Client.prototype.get = thunkify(Client.prototype.get_cb);
Client.prototype.request = thunkify(Client.prototype.request_cb);
Client.prototype.send = thunkify(Client.prototype.send_cb);

module.exports = Client;
module.exports.encode = encode;
module.exports.FromGrid = FromGrid;
module.exports.minMulticast = minMulticast;
