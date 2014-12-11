// Ugrid

var assert = require('assert');
var net = require('net');
var readline = require('readline');
var thunkify = require('thunkify');

module.exports = function(arg) {
	var host = arg.host;
	var port = arg.port;
	var data = arg.data ? arg.data : {};
	var my_uuid, grid, my_token;
	var pending_cmd = {};
	var grid_events = {};
	var cmd_cnt = 0;

	function send_cmd(json) {
		var cmd = {
			cmd: json.cmd,
			from: my_uuid ? my_uuid : null,
			token: my_token ? my_token : null,
			cmd_id: json.cmd_id ? json.cmd_id : get_next_cmd_id(),
			data: json.data
		}
		if (json.process)
			pending_cmd[cmd.cmd_id] = {process: json.process};
		grid.write(JSON.stringify(cmd) + '\n');
	}

	this.connect_cb = function(callback) {
		grid = net.connect(port, host, function() {
			var rl = readline.createInterface(grid, grid);

			send('connect', data, function(err, res) {
				if (res) {
					my_uuid = res.uuid;
					my_token = res.token;
				}
				callback(err, res);
			});
			
			rl.on('line', function(d) {
				try {
					var o = JSON.parse(d);
					if (o.data && o.data.err)
						throw o.data.err;
					if (o.cmd in grid_events) {	      // command from server
						grid_events[o.cmd](o);
					} else if (o.cmd_id in pending_cmd) { // answer to previous cmd
						assert(o.data);
						pending_cmd[o.cmd_id].process(o.data.err, o.data.result, o.from);
						delete(pending_cmd[o.cmd_id]);
					}
				} catch (error) {
					console.trace('on received line:\n' + d + '\nError: ' + error);
					process.exit(1);
				}
			});
			grid.on('end', function() {
				console.error('Server closed connection');
				process.exit(1);
			});
		});
	};

	this.on = function(event, callback) {
		grid_events[event] = callback;
	};

	this.disconnect = function() {
		grid.end();
	};

	var send = this.send_cb = function(cmd, data, callback) {
		var o = {cmd: cmd, data: data};
		if (o.cmd === 'answer') {
			o.cmd_id = o.data.cmd_id;
		}
		if (callback) {
			o.process = function(error, result, from) {
				callback(error, result, from);
			};
		}
		send_cmd(o);
	};

	var get_next_cmd_id = this.get_next_cmd_id = function () {
		if (cmd_cnt++ == 1000000) {
			console.log("cmd_id overflow!");
			cmd_cnt = 0;
		}
		return cmd_cnt;
	};

	this.connect = thunkify(this.connect_cb);
	this.send = thunkify(this.send_cb);
};
