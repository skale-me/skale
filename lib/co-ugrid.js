// Co ugrid routines
var thunkify = require('thunkify');
var Ugrid = require('../lib/ugrid-client.js');

module.exports = function(arg) {
	var grid =  new Ugrid(arg);

	this.connect_cb = grid.connect_cb;
	this.send_cb = grid.send_cb;
	this.on = grid.on;
	this.disconnect = grid.disconnect;

	this.connect = thunkify(grid.connect_cb);
	this.send = thunkify(grid.send_cb);
}
