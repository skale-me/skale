/* ugrid client side library */

'use strict';

var net = require('net');
var util = require('util');
var stream = require('stream');
var websocket = require('websocket-stream');    // Keep this order for browserify

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
}

function FromGrid() {
	if (!(this instanceof FromGrid))
		return new FromGrid();
	stream.Transform.call(this, {objectMode: true});
	this._buf = null
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
}

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

		if (msg.cmd === 'reply') {
			if (this.client.pending[msg.cid]) {
				this.client.pending[msg.cid](msg.error, msg.data);
			} else {
				console.warn('[' + this.client.id +  '] unwanted reply: ' + util.inspect(msg));
			}
			done();
		} else if (this.subscriber[msg.cmd]) {
			this.subscriber[msg.cmd].write(msg.data, done);
		} else if (this.client.event[msg.cmd]) {
			//this.client.event[msg.cmd](msg, done);
			this.client.event[msg.cmd](msg);
			done();
		} else done();
	} catch (error) {
		console.error(error);
		console.error(chunk.slice(8).toString());
		if (msg.cmd === 'reply')
			console.log(this.client.pending[msg.cid].toString());
		process.exit(1);
	}
}

function PubStream(client, dest, cmd) {
	if (!(this instanceof PubStream))
		return new PubStream(client, dest, cmd);
	stream.Transform.call(this, {objectMode: true});
	this.dest = dest;
	this.cmd = cmd;
	this.pipe(client.output);
}
util.inherits(PubStream, stream.Transform);

PubStream.prototype._transform = function (chunk, encoding, done) {
	done(null, {cmd: this.cmd, id: this.dest.id, data: chunk.toString()});
};

function Client(opt, callback) {
	if (!(this instanceof Client))
		return new Client(opt, callback);
	opt = opt || {};
	opt.host = opt.host || 'localhost';
	opt.port = opt.port || 12346;
	var self = this;
	this.pending = [];
	this.event = {};
	this.hostId = {};
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
		}
		if (callback) callback(err, data);
	});
	this.sock.on('end', function () {
		if (self.event.close) self.event.close();
		process.exit(0);
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
	var cid = msg.cid || this.cid++;
	msg.cid = cid;
	if (callback) this.pending[cid] = callback;
	if (uuid) {
		if (this.hostId[uuid]) msg.id = this.hostId[uuid];
		else return this._getId(uuid, 3, msg, callback);
	}
	msg.from = this.id;
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

Client.prototype.request_cb = function (dest, data, callback) {
	this.send_cb(dest.uuid, {cmd: 'request', ufrom: this.uuid, data: data}, callback);
};

Client.prototype.reply = function (msg, error, data) {
	// if (msg.cmd !== 'request') throw 'invalid msg reply';
	var uuid = msg.ufrom;
	msg.cmd = 'reply';
	msg.id = msg.from;
	msg.ufrom = undefined;
	msg.data = data;
	msg.error = error;
	this.send_cb(uuid, msg);
}

Client.prototype.pipe = function (topic, stream) {
	this.consumer.subscriber[topic] = stream;
	return stream;
};

Client.prototype.on = function (ev, callback) {
	this.event[ev] = callback;
};

Client.prototype.createWriteStream = function (dest, name) {
	return new PubStream(this, dest, name);
};

Client.prototype.end  = function () {
	this.sock.end();
};

module.exports = Client;
module.exports.encode = encode;
module.exports.FromGrid = FromGrid;
