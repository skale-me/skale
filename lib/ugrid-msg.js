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

function Decoder(opt) {
	if (!(this instanceof Decoder))
		return new Decoder(opt);
	stream.Transform.call(this, {objectMode: true});
	this._buf = null;
}

Decoder.prototype._transform = function(chunk, encoding, done) {
	var dest, len, data, buf, offset = 0;

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
		// dest = buf.readUInt32LE(0, true);
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

function encodeStr(str, id) {
	var len = Buffer.byteLength(str),
	    buf = new Buffer(len + 8);
	buf.writeUInt32LE(id, 0);
	buf.writeUInt32LE(len, 4);
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
