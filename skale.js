#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

var child_process = require('child_process');
var fs = require('fs');
var net = require('net');
var DDPClient = require('ddp');
var login = require('ddp-login');
var netrc = require('netrc');

var help='Usage: skale [options] <command> [<args>]\n' +
'\n' +
'Create, run, deploy clustered node applications\n' +
'\n' +
'Commands:\n' +
'  create <app>		Create a new application\n' +
'  run [<args>...]	Run application\n' +
'  deploy [<args>...]	Deploy application\n' +
'  status		print status of local skale cluster\n' +
'  stop			Stop local skale cluster\n' +
'\n' +
'Options:\n' +
'  -f, --file		program to run (default: package name)\n' +
'  -h, --help		Show help\n' +
'  -m, --memory MB	set the memory space limit per worker (default 4000 MB)\n' +
'  -r, --remote		run in the cloud instead of locally\n' +
'  -V, --version		Show version\n' +
'  -w, --worker num	set the number of workers (default 2)\n';

var argv = require('minimist')(process.argv.slice(2), {
	string: [
		'c', 'config',
		'f', 'file',
		'H', 'host',
		'k', 'key',
		'p', 'port',
		'm', 'memory',
		'w', 'worker',
	],
	boolean: [
		'h', 'help',
		'r', 'remote',
		'V', 'version',
	],
	default: {
		H: 'skale.me', 'host': 'skale.me',
		p: '8888', 'port': 8888,
	}
});


if (argv.h || argv.help) {
	console.log(help);
	process.exit();
}
if (argv.V || argv.version) {
	var pkg = require('./package');
	console.log(pkg.name + '-' + pkg.version);
	process.exit();
}

var configPath = argv.c || argv.config || process.env.SKALE_CONFIG || process.env.HOME + '/.skalerc';
var config = loadConfig(argv);
var proto = config.ssl ? require('https') : require('http');
var memory = argv.m || argv.memory || 4000;
var worker = argv.w || argv.worker || 2;

switch (argv._[0]) {
	case 'create':
		create(argv._[1]);
		break;
	case 'deploy':
		deploy(argv._.splice(1));
		break;
	case 'run':
		run_local(argv._.splice(1));
		break;
	case 'status':
		status_local();
		break;
	case 'stop':
		//stop_local_server();
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
	console.log('create local repository');
	child_process.execSync('git init');

	var pkg = {
		name: name,
		version: '0.1.0',
		private: true,
		keywords: [ 'skale' ],
		dependencies: {
			'skale-engine': '^0.6.0'
		}
	};
	fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2));
	var src = '#!/usr/bin/env node\n' +
		'\n' +
		'var sc = require(\'skale-engine\').context();\n' +
		'\n' +
		'sc.parallelize([\'Hello world\']).collect().then(function (res) {\n' +
		'	console.log(res);\n' +
		'	sc.end();\n' +
		'});\n';
	fs.writeFileSync(name + '.js', src);
	var gitIgnore = 'node_modules\nnpm-debug.log*\n';
	fs.writeFileSync('.gitignore', gitIgnore);
	var npm = child_process.spawnSync('npm', ['install'], {stdio: 'inherit'});
	if (npm.status) die('skale create error: npm install failed');
	console.log('Project ${name} is now ready.\n' +
		'Please change directory to ' + name + ': "cd ' + name + '"\n' +
		'To run your app: "skale run"\n' +
		'To modify your app: edit ' + name + '.js');
}

function die(err) {
	console.error(help);
	console.error(err);
	process.exit(1);
}

function loadConfig(argv) {
	var conf = {}, save = false;
	try { conf = JSON.parse(fs.readFileSync(configPath)); } catch (error) { save = true; }
	//conf.host = argv.H || argv.host || process.env.SKALE_HOST || conf.host;
	//conf.port = argv.p || argv.port || process.env.SKALE_PORT || conf.port;
	//conf.key = argv.k || argv.key || conf.key;
	//conf.ssl = argv.s || argv.ssl || (conf.ssl ? true : false);
	process.env.SKALE_TOKEN = process.env.SKALE_TOKEN || conf.token;
	if (save || argv._[0] == 'init') saveConfig(conf);
	return conf;
}

function saveConfig(config) { fs.writeFileSync(configPath, JSON.stringify(config, null, 2)); }

function status_local() {
	var child = child_process.execFile('/bin/ps', ['ux'], function (err, out) {
		var lines = out.split(/\r\n|\r|\n/);
		for (var i = 0; i < lines.length; i++)
			if (i == 0 || lines[i].match(/ skale-/)) console.log(lines[i].trim());
	});
}

function run_local(args) {
	var pkg = JSON.parse(fs.readFileSync('package.json'));
	var cmd = argv.f || argv.file || pkg.name + '.js';
	args.splice(0, 0, cmd);
	child_process.spawn('node', args, {stdio: 'inherit'});
}

function deploy(args) {
	var key = args[0] || process.env.SKALE_KEY || '';
	var host = args[1] || process.env.SKALE_SERVER || 'localhost';
	var port = args[2] || process.env.SKALE_PORT || 3000;

	console.log('# key:', key);
	console.log('# server:', host, port);

	var ddpclient = new DDPClient({
		// All properties optional, defaults shown
		host : host,
		port : port,
		ssl  : false,
		autoReconnect : true,
		autoReconnectTimer: 500,
		maintainCollections : true,
		ddpVersion: '1',  // ['1', 'pre2', 'pre1'] available
		useSockJs: true,
		url: 'wss://example.com/websocket'
	});

	ddpclient.connect(function (err, isreconnect) {
		if (err) throw err;
		console.log('connected to meteor');
		login(ddpclient, {env: 'SKALE_TOKEN'}, afterLogin);
	});

	function afterLogin(err, userInfo) {
		if (err) throw err;
		var token = userInfo.token;
		if (userInfo.token != config.token) {
			config.token = userInfo.token;
			saveConfig(config);
		}
		console.log(userInfo);
		console.log('reading package.json');
		var pkg = JSON.parse(fs.readFileSync('package.json'));
		var name = pkg.name;
		child_process.exec('git remote get-url skale', function (err, stdout, stderr) {
			if (!err) return deploy();
			ddpclient.call('etls.add', [{name: name}], function (err, res) {
				if (err) throw new Error(err);
				var a = res.url.split('/');
				var login = a[a.length - 2];
				var host = a[2].replace(/:.*/, '');
				var passwd = res.token;
				var rc = {};
				rc[host] = {login: login, password: passwd};
				netrc.save(rc);
				child_process.execSync('git remote add skale ' + res.url);
				deploy();
			});

			function deploy() {
				console.log('deploying ETL');
				child_process.execSync('git add -A .; git commit -m "automatic commit"; git push skale master');
				ddpclient.call('etls.deploy', [{name: name}], function (err, res) {
					console.log('ETL is being deployed ...')
					process.exit(0);
				});
			}
		});
	}
}


function run_remote(args) {
	var name = process.cwd().split('/').pop();
	var postdata = JSON.stringify({name: name, args: args});

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
}
