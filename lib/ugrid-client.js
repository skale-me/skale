/*
  ugrid client side library.
*/
var net = require('net');
var ugridMsg = require('./ugrid-msg.js');
var thunkify = require('thunkify');

module.exports = function(arg) {
	var events = {};
	var sock;
	var pending = {};
	var cid = 0;
	var cidMax = 10000000;
	var myId;
	var noError = false;

	events.reply = function(o) {
		pending[o.cid](o.error, o.data);
		delete(pending[o.cid]);
	};
	events.request = function() {};

	this.connect_cb = function(callback) {
		sock = net.connect(arg.port, arg.host, function() {
			var decoder = ugridMsg.Decoder();
			sock.pipe(decoder);

			send_cb({cmd: 'connect', data: arg.data}, function(err, res) {
				myId = res.id;
				callback(err, res);
			});

			decoder.on('Message', function(to, len, data) {
				o = JSON.parse(data.slice(8));
				events[o.cmd](o);
			});
		});
		sock.on('end', function() {
			if (!noError) {
				console.error('Unexpected connection close');
				process.exit(1);
			}
		});
		sock.on('error', function(error) {
			console.error('IO: Connection ' + error);
			process.exit(1);
		});
	};

	this.on = function(event, callback) {
		events[event] = callback;
	};

	this.disconnect = function() {
		noError = true;
		sock.end();
	};
	
	this.send_cb = send_cb;
	
	function send_cb(o, callback) {
		if (!o.cid) {
			o.cid = (cid == cidMax ? 0 : cid++);
			pending[o.cid] = callback;
		}
		o.from = myId;
		sock.write(ugridMsg.encode(o));
	}

	this.connect = thunkify(this.connect_cb);
	this.send = thunkify(this.send_cb);
}
