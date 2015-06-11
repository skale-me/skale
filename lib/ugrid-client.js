/* ugrid client side library */

'use strict';

var net = require('net');
var util = require('util');
var stream = require('stream');
var trace = require('line-trace');
var events = require('events');
var thenify = require('thenify').withCallback;
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

function ToGrid(debug) {
	if (!(this instanceof ToGrid))
		return new ToGrid(debug);
	stream.Transform.call(this, {objectMode: true});
	this.debug = debug;
}
util.inherits(ToGrid, stream.Transform);

ToGrid.prototype._transform = function (msg, encoding, done) {
	if (this.debug) console.log('\n# Send: %j', msg);
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

function Consumer(debug) {
	if (!(this instanceof Consumer))
		return new Consumer(debug);
	stream.Transform.call(this, {objectMode: true});
	this.subscriber = {};
	this.debug = debug;
}
util.inherits(Consumer, stream.Transform);

Consumer.prototype._transform = function (chunk, encoding, done) {
	var msg = JSON.parse(chunk.slice(8));

	if (msg.ufrom && !this.client.hostId[msg.ufrom])
		this.client.hostId[msg.ufrom] = msg.from;

	if (this.debug) console.log('\n# Received: %j', msg);
	if (msg.cmd == 'reply') {
		if (this.client.pending[msg.cid]) {
			this.client.pending[msg.cid](msg.error, msg.data);
			delete this.client.pending[msg.cid];
		} else {
			console.warn('[' + this.client.id +  '] unwanted reply: %j', msg);
		}
		done();
	} else if (this.subscriber[msg.cmd]) {
		this.subscriber[msg.cmd].write(JSON.stringify(msg.data), done);
	} else {
		this.client.emit(msg.cmd, msg);
		done();
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
			this.client.send(0, {cmd: 'id', data: this.dest.uuid}, function (err, res) {
				if (err || res === undefined)
					throw new Error('PubStream error: ' + err);
				self.client.hostId[self.dest.uuid] = self.id = res;
				done(null, {cmd: self.cmd, id: self.id, data: chunk.toString()});
			});
		} else {
			this.client.send(0, {cmd: 'tid', data: this.cmd}, function (err, res) {
				if (err || res === undefined)
					throw new Error('PubStream error: ' + err);
				self.client.topicId[self.cmd] = self.id = minMulticast + res;
				done(null, {cmd: self.cmd, id: self.id, data: chunk.toString()});
			});
		}
	} else
		done(null, {cmd: this.cmd, id: this.id, data: chunk.toString()});
};

function WriteStream(client, name, dest) {
	if (!(this instanceof WriteStream))
		return new WriteStream(client, name);
	stream.Transform.call(this, {objectMode: true});
	this.client = client;
	this.name = name;
	this.dest = dest;
	this.ended = false;
	var self = this;

	self.on('end', function (ignore) {
		self.ignore = ignore;
		self.ended = true;
		self.end();
	});
}

util.inherits(WriteStream, stream.Transform);

// Flow control is performed through reply from remote
WriteStream.prototype._transform = function (chunk, encoding, done) {
	this.client.send(this.dest, {
		cmd: 'request',
		data: {cmd: 'stream', stream: this.name, data: chunk}
	}, done);
};

WriteStream.prototype._flush = function(done) {
	try {
		this.client.send(this.dest, {
			cmd: 'request',
			data: {cmd: 'stream', stream: this.name, data: null, ignore: this.ignore}
		}, done);
	} catch (err) {}
};

function ReadStream (client, name) {
	if (!(this instanceof ReadStream))
		return new ReadStream(client, name);
	stream.Transform.call(this, {objectMode: true});
	this.client = client;
	this.name = name;
}
util.inherits(ReadStream, stream.Transform);

ReadStream.prototype._transform = function (chunk, encoding, done) {
	done(null, chunk);
	// try { done(null, chunk); } catch (err) {}
};

function Client(opt, callback) {
	if (!(this instanceof Client))
		return new Client(opt, callback);
	events.EventEmitter.call(this);
	var inBrowser = (typeof window != 'undefined');
	opt = opt || {};
	if (!opt.ws) opt.ws = inBrowser ? true : process.env.UGRID_WS;
	if (!opt.host && !inBrowser) opt.host = process.env.UGRID_HOST;
	if (!opt.port && !inBrowser) opt.port = process.env.UGRID_PORT;
	if (!opt.debug && !inBrowser) opt.debug = process.env.UGRID_DEBUG;
	opt.host = opt.host || 'localhost';
	opt.port = opt.port || (opt.ws ? 12348 : 12346);
	var self = this;
	this.pending = {};
	this.hostId = {};
	this.topicId = {};
	this.cid = 0;
	if (opt.ws) {
		this.sock = websocket('ws://' + opt.host + ':' + opt.port);
	} else {
		this.sock = net.connect(opt.port, opt.host);
		this.sock.setNoDelay();
	}
	this.debug = opt.debug;
	this.input = new FromGrid();
	this.output = new ToGrid(opt.debug);
	this.consumer = new Consumer(opt.debug);
	this.consumer.client = this;
	this.sock.pipe(this.input).pipe(this.consumer);
	this.output.pipe(this.sock);
	this.send(0, {cmd: 'connect', data: opt.data}, function (err, data) {
		var i, d;
		if (data) {
			self.id = data.id;
			self.uuid = data.uuid;
			self.emit('connect', data);
			if (data.devices) {		// Cache remote ids
				for (i = 0; i < data.devices.length; i++) {
					d = data.devices[i];
					self.hostId[d.uuid] = d.id;
				}
			}
		}
		if (callback) callback(err, data);
	});
	self.on('notify', function (msg) {      // Cache remote id
		self.hostId[msg.data.uuid] = msg.data.id;
	});
	this.sock.on('end', function () {
		self.emit('close');
	});
	this.sock.on('close', function () {
		self.emit('close');
	});
	this.sock.on('error', function (err) {
		if (inBrowser) return;
		self.emit('error', err);
	});
}
util.inherits(Client, events.EventEmitter);

Client.prototype._getId = function (uuid, nTry, msg, callback) {
	var self = this;
	this.send(0, {cmd: 'id', data: uuid}, function (err, res) {
		if (res) {
			msg.id = self.hostId[uuid] = res;
			msg.from = self.id;
			self.output.write(msg);
		} else {
			if (--nTry < 0) {
				if (self.pending[msg.cid]) {
					console.error('_getId failed');
					self.pending[msg.cid]('_getIDd failed');
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

Client.prototype.send = thenify(function (uuid, msg, callback) {
	try {
		msg.cid = this.cid++;
		if (callback) this.pending[msg.cid] = callback;
		msg.from = this.id;
		if (uuid) {
			if (this.hostId[uuid]) msg.id = this.hostId[uuid];
			else return this._getId(uuid, 3, msg, callback);
		}
		this.output.write(msg);
	} catch(err) {
		//console.error(msg);
		throw new Error('send error');
	}
});

Client.prototype.devices = thenify(function (o, max, callback) {
	var self = this;
	this.send(0, {cmd: 'devices', data: {query: o, max: max}}, function (err, dev) {
		for (var i in dev)
			self.hostId[dev[i].uuid] = dev[i].id;
		callback(err, dev);
	});
});

Client.prototype.get = thenify(function (uuid, callback) {
	this.send(0, {cmd: 'get', data: uuid}, callback);
});

Client.prototype.notify = function (uuid) {
	this.output.write({cmd: 'notify', data: uuid});
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
		this.send(0, {cmd: 'tid', data: topic}, function (err, res) {
			if (err || res === undefined) return;
			self.topicId[topic] = res;
			self.output.write({cmd: topic, id: minMulticast + res, data: content});
		});
	} else
		this.output.write({cmd: topic, id: minMulticast + this.topicId[topic], data: content});
};

Client.prototype.request = thenify(function (dest, data, callback) {
	this.send(dest.uuid, {cmd: 'request', ufrom: this.uuid, data: data}, callback);
});

Client.prototype.reply = function (msg, error, data) {
	if (msg.cmd !== 'request') {
		//console.log(msg);
		throw new Error('wrong msg');
	}
	//console.assert(msg.cmd === 'request');
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

Client.prototype.createReadStream = function (name) {
	return new ReadStream(this, name);
};

Client.prototype.createWriteStream = function (name, dest) {
	return new WriteStream(this, name, dest);
};

//Client.prototype.createWriteStream = function (name, dest) {
//	return new PubStream(this, name, dest);
//};

Client.prototype.end = Client.prototype._end = function () {
	this.output.end({cmd: 'end'});
};

module.exports = Client;
module.exports.encode = encode;
module.exports.FromGrid = FromGrid;
module.exports.minMulticast = minMulticast;
