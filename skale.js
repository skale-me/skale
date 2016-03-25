#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const help=`Usage: skale [options] <command> [<args>]

Create, run, deploy clustered node applications

Commands:
  create <app>		Create a new application
  run [<args>...]	Run application
  deploy [<args>...]	Deploy application (coming soon)
  status		print status of local skale cluster
  stop			Stop local skale cluster

Options:
  -h, --help		Show help
  -V, --version		Show version
`;

const child_process = require('child_process');
const fs = require('fs');
const net = require('net');

const argv = require('minimist')(process.argv.slice(2), {
	string: ['c', 'config', 'H', 'host', 'p', 'port', 'k', 'key'],
	boolean: ['h', 'help', 'V', 'version', 's', 'ssl'],
});

var skale_port = 12346;

if (argv.h || argv.help) {
	console.log(help);
	process.exit();
}
if (argv.V || argv.version) {
	var pkg = require('./package');
	console.log(pkg.name + '-' + pkg.version);
	process.exit();
}

const config = load(argv);
const proto = config.ssl ? require('https') : require('http');

switch (argv._[0]) {
	case 'create':
		create(argv._[1]);
		break;
	case 'demo':
		run_remote(__dirname + '/demo/' + argv._[1], argv._.splice(2));
		break;
	case 'run':
		run_local(argv._.splice(1));
		break;
	case 'status':
		status_local();
		break;
	case 'stop':
		stop_local_server();
		break;
	default:
		die('Error: invalid command: ' + argv._[0]);
}

function create(name) {
	console.log('create application ' + name);
	try {
		fs.mkdirSync(name);
	} catch (error) {
		die('skale create error: ' + error.message);
	}
	process.chdir(name);
	const pkg = {
		name: name,
		version: '0.0.0',
		private: true,
		keywords: [ 'skale' ],
		dependencies: {
			'skale-engine': '^0.3.2'
		}
	};
	fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2));
	var src = `#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize(['Hello world'])
  .collect()
  .on('data', console.log)
  .on('end', sc.end);
`;
	fs.writeFileSync(name + '.js', src);
	const npm = child_process.spawnSync('npm', ['install'], {stdio: 'inherit'});
	if (npm.status) die('skale create error: npm install failed');
	console.log(`Project ${name} is now ready.
Pleas change directory to ${name}: "cd ${name}"
To run your app: "skale run"
To modify your app: edit ${name}.js`)
}

function die(err) {
	console.error(help);
	console.error(err);
	process.exit(1);
}

function load(argv) {
	var conf = {}, save = false;
	var path = argv.c || argv.config || process.env.SKALE_CONFIG || process.env.HOME + '/.skalerc';
	try { conf = JSON.parse(fs.readFileSync(path)); } catch (error) { save = true; }
	conf.host = argv.H || argv.host || process.env.SKALE_HOST || conf.host || die('Error: missing host');
	conf.port = argv.p || argv.port || process.env.SKALE_PORT || conf.port || die('Error: missing port');
	conf.key = argv.k || argv.key || conf.key;
	conf.ssl = argv.s || argv.ssl || (conf.ssl ? true : false);
	if (save || argv._[0] == 'init') fs.writeFileSync(path, JSON.stringify(conf, null, 2));
	return conf;
}

function start_skale(done) {
	const out = fs.openSync('skale-server.log', 'a');
	const err = fs.openSync('skale-server.log', 'a');
	const child = child_process.spawn('node_modules/skale-engine/bin/server.js', ['-l', '2'], {
		detached: true,
		stdio: ['ignore', out, err]
	});
	child.unref();
	try_connect(5, 1000, done);
}

function try_connect(nb_try, timeout, done) {
	const sock = net.connect(skale_port);
	sock.on('connect', function () {
		sock.end();
		done(null);
	});
	sock.on('error', function (err) {
		if (--nb_try <= 0) return done('skale-server not ok');
		setTimeout(function () { try_connect(nb_try, timeout, done); }, timeout);
	});
}

function stop_local_server() {
	const child = child_process.execFile('/usr/bin/pgrep', ['skale-server'], function (err, pid) {
		if (! pid) return;
		process.kill(pid.trim());
	});
}

function status_local() {
	const child = child_process.execFile('/bin/ps', ['ux'], function (err, out) {
		var lines = out.split(/\r\n|\r|\n/);
		for (var i = 0; i < lines.length; i++)
			if (i == 0 || lines[i].match(/ skale-/)) console.log(lines[i]);
	});
}

function run_local(args) {
	const pkg = JSON.parse(fs.readFileSync('package.json'));
	var cmd = pkg.name + '.js';
	args.splice(0, 0, cmd);
	try_connect(0, 0, function (err) {
		if (!err) return run_app();
		start_skale(run_app);
	});
	function run_app() { child = child_process.spawn('node', args, {stdio: 'inherit'}); }
}

function run_remote(src, args) {
	fs.readFile(src, {encoding: 'utf8'}, function (err, data) {
		if (err) throw err;

		var postdata = JSON.stringify({src: data, args: args});

		var options = {
			hostname: config.host,
			port: config.port,
			path: '/run',
			method: 'POST',
			headers: {
				'X-Auth': config.key,
				'Content-Type': 'application/json',
				'Content-Length': Buffer.byteLength(postdata)
			}
		};

		var req = proto.request(options, function (res) {
			res.setEncoding('utf8');
			res.pipe(process.stdout);
		});

		req.on('error', function (err) {throw err;});
		req.end(postdata);
	});
}
