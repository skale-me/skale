/*
  Implement a simple stream binary protocol for fast messaging

  Usage:
    var pubsub = new PubSub(readable_stream);
	readable_stream.pipe(pubsub);
	pubsub.on('Message', function(to, dlen, msg) {
		...
	});
*/
'use strict';

var stream = require('stream');
var util = require('util');

util.inherits(Decoder, stream.Transform);

function Decoder(options) {
	if (!(this instanceof Decoder))
		return new Decoder(options);
	stream.Transform.call(this, options);
	this.buf = null;
}

Decoder.prototype._transform = function(chunk, encoding, done) {
	var dest, len, data, buf, offset = 0;

	if (this.buf) {
		chunk = Buffer.concat([this.buf, chunk], this.buf.length + chunk.length);
		this.buf = null;
	}
	do {
		buf = chunk.slice(offset);
		if (buf.length < 8) {
			this.buf = buf;
			break;
		}
		dest = buf.readUInt32LE(0);
		len = buf.readUInt32LE(4);
		if (buf.length < 8 + len) {
			this.buf = buf;
			break;
		}
		data = buf.slice(0, 8 + len);
		this.emit('Message', dest, len, data);
		offset += 8 + len;
	} while (offset < chunk.length);
	done();
};

function encodeStr(str, id) {
	var buf = new Buffer(str.length + 8);
	buf.writeUInt32LE(id, 0);
	buf.writeUInt32LE(str.length, 4);
	buf.write(str, 8);
	return buf;
}

function encode(msg) {
	return encodeStr(JSON.stringify(msg), msg.id);
}

module.exports = {
	Decoder: Decoder,
	encode: encode,
	encodeStr: encodeStr
};
