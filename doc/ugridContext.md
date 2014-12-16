## UgridContext class
Example:

	var UgridContext = require('ugrid-context');
	var ugrid = new UgridContext({host: 'localhost', port: 12346});

or in a shorter form:

	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});

## ugrid.init_cb([callback])
Example:

	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});
	ugrid.init(function () {
		console.log('init done, ugrid ready');
	});

Or with using co:

	var co = require('co');
	var ugrid = require('ugrid-context')({host: 'localhost', port: 12346});

	co(function *() {
		yield ugrid.init();
		console.log(init done, ugrid ready');
	})();
