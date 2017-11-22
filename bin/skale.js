#!/usr/bin/env node
'use strict';

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const child_process = require('child_process');
const fs = require('fs');
const DDPClient = require('ddp');
const login = require('ddp-login');
const netrc = require('netrc');

const help = 'Usage: skale [options] <command> [<args>]\n' +
'\n' +
'Create, test, deploy, run clustered NodeJS applications\n' +
'\n' +
'Commands:\n' +
'  create <app>        Create a new application\n' +
'  test [<args>...]    Run application on local host\n' +
'  deploy [<args>...]  Deploy application on skale cloud\n' +
'  run [<args>...]     Run application on skale cloud\n' +
'  attach              Attach to a running application\n' +
'  list                List deployed applications\n' +
'  log                 Print log of an application\n' +
'  signup              Create an account on skale cloud\n' +
'  status [<name>]     Print status of application on skale cloud\n' +
'  stop                Stop application on skale cloud\n' +
'\n' +
'Options:\n' +
'  -d, --debug         Enable debug traces\n' +
'  -f, --file          Set program to run (default: package name)\n' +
'  --force             Force action to occur, despite warning\n' +
'  -h, --help          Print help and quit\n' +
'  -m, --memory MB     Set the memory space limit per worker (default 4000 MB)\n' +
'  -r, --remote        Run in the cloud instead of locally\n' +
'  -V, --version       Print version and quit\n' +
'  -w, --worker num    Set the number of workers (default 2)\n';

const argv = require('minimist')(process.argv.slice(2), {
  string: [
    'c', 'config',
    'f', 'file',
    'm', 'memory',
    'w', 'worker',
  ],
  boolean: [
    'd', 'debug',
    'force',
    'h', 'help',
    'V', 'version',
  ],
  default: {}
});


if (argv.h || argv.help) {
  console.log(help);
  process.exit();
}
if (argv.V || argv.version) {
  const pkg = require('./package');
  console.log(pkg.name + '-' + pkg.version);
  process.exit();
}
if (argv.d || argv.debug) {
  process.env.SKALE_DEBUG = 2;
}

const configPath = argv.c || argv.config || process.env.SKALE_CONFIG || process.env.HOME + '/.skalerc';
const config = load_config(argv);
const rc = netrc();
const start = process.hrtime();
let trace;

if (process.env.SKALE_DEBUG > 1) {
  trace =  function trace() {
    const args = Array.prototype.slice.call(arguments);
    const elapsed = process.hrtime(start);
    args.unshift('[skale ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
    console.error.apply(null, args);
  };
} else {
  trace = function noop() {};
}

switch (argv._[0]) {
case 'attach':
  attach();
  break;
case 'create':
  create(argv._[1]);
  break;
case 'deploy':
  deploy(argv._.splice(1));
  break;
case 'list':
  list(argv._.splice(1));
  break;
case 'log':
  log.apply(null, argv._.splice(1));
  break;
case 'run':
  run_remote(argv._.splice(1));
  break;
case 'signup':
  console.log('signup: not implemented yet');
  break;
case 'status':
  status(argv._[1]);
  break;
case 'stop':
  stop();
  break;
case 'test':
  run_local(argv._.splice(1));
  break;
default:
  die('Error: invalid command: ' + argv._[0]);
}

function checkName(name) {
  return /^[A-Za-z][A-Za-z0-9_-]+$/.test(name);
}

function create(name) {
  if (!checkName(name)) die('skale create error: invalid name ' + name);
  console.log('create application ' + name);
  try {
    fs.mkdirSync(name);
  } catch (error) {
    die('skale create error: ' + error.message);
  }
  process.chdir(name);
  console.log('create local repository');
  try {
    child_process.execSync('git init');
  } catch (err) {
    console.log('It looks like you may not have git installed.  Skale needs that to run.  See https://git-scm.com/ for installation.');
    return;
  }

  const pkg = {
    name: name,
    version: '0.1.0',
    private: true,
    keywords: [ 'skale' ],
    dependencies: {
      'skale-engine': '^0.6.1'
    }
  };
  fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2));
  const src = '#!/usr/bin/env node\n' +
    '\n' +
    'const sc = require(\'skale-engine\').context();\n' +
    '\n' +
    'sc.parallelize([\'Hello world\']).collect().then(function (res) {\n' +
    ' console.log(sc.worker.length + \' workers, res:\', res);\n' +
    ' sc.end();\n' +
    '});\n';
  fs.writeFileSync(name + '.js', src);
  const gitIgnore = 'node_modules\nnpm-debug.log*\n.npm-install-changed.json\n';
  fs.writeFileSync('.gitignore', gitIgnore);
  const npm = child_process.spawnSync('npm', ['install'], {stdio: 'inherit'});
  if (npm.status) die('skale create error: npm install failed');


  console.log('\n----------------------------------' +
    '\nProject ' + name + ' is now ready!\n' +
    'Please change directory to ' + name + ': "cd ' + name + '"\n' +
    'To run your app locally: "skale test"\n' +
    'To modify your app edit the file called ' + name + '.js');
}

function die(err) {
  console.error(err);
  process.exit(1);
}

function load_config(argv) {
  let conf = {};
  let save = false;
  try { conf = JSON.parse(fs.readFileSync(configPath)); } catch (error) { save = true; }
  process.env.SKALE_TOKEN = process.env.SKALE_TOKEN || conf.token;
  if (save || argv._[0] == 'init') save_config(conf);
  return conf;
}

function save_config(config) {
  fs.writeFile(configPath, JSON.stringify(config, null, 2), function (err) {
    if (err) die('Could node write ' + configPath + ':', err);
  });
}

function run_local(args) {
  const pkg = JSON.parse(fs.readFileSync('package.json'));
  const cmd = argv.f || argv.file || pkg.name + '.js';
  args.splice(0, 0, cmd);
  child_process.spawn('node', args, {stdio: 'inherit'});
}

function skale_session(callback) {
  const host = process.env.SKALE_SERVER || 'apps.skale.me';
  const port = process.env.SKALE_PORT || 443;

  const ddp = new DDPClient({
    // All properties optional, defaults shown
    host : host,
    port : port,
    ssl  : !process.env.SKALE_NOSSL,
    autoReconnect : true,
    autoReconnectTimer: 500,
    maintainCollections : true,
    ddpVersion: '1',  // ['1', 'pre2', 'pre1'] available
    useSockJs: true,
    url: 'wss://example.com/websocket'
  });

  ddp.connect(function (err, isreconnect) {
    if (err) return callback(err, ddp, isreconnect);
    login(ddp, {env: 'SKALE_TOKEN', retry: 2}, function (err, userInfo) {
      if (err) return callback(err, ddp, isreconnect);
      if (userInfo.token != config.token) {
        config.token = userInfo.token;
        save_config(config);
      }
      trace('connected to', host, port);
      return callback(err, ddp, isreconnect);
    });
  });

  ddp.on('socker-close', function () {trace('disconnected');});
}

function deploy() {
  skale_session(function (err, ddp) {
    if (err) {
      switch (err.reason) {
      case 'User not found':
        die('User not found');
        break;
      default:
        die(err.toString());
      }
    }
    const pkg = JSON.parse(fs.readFileSync('package.json'));
    const name = pkg.name;

    ddp.call('etls.add', [{name: name}], function (err, res) {
      if (err) die('Could not create application ' + name + ':', err);
      trace('application added on server', name);
      const a = res.url.split('/');
      const login = a[a.length - 2];
      const host = a[2].replace(/:.*/, '');
      const passwd = res.token;
      rc[host] = {login: login, password: passwd};
      netrc.save(rc);
      child_process.exec('git remote remove skale; git remote add skale "' + res.url + '"; git add -A .; git commit -m "automatic commit"; git pull --rebase -Xours skale master; git push skale master', function (err) {
        if (err) die('deploy error: ' + err);
        trace('application transfered using git');
        ddp.call('etls.deploy', [{name: name}], function (err) {
          if (err) console.error(err);
          else console.log(name + ' deployed');
          trace('done');
          ddp.close();
        });
      });
    });
  });
}

function list() {
  skale_session(function (err, ddp) {
    if (err) die('Could not connect:', err);
    const user = Object.keys(ddp.collections.users)[0];
    ddp.subscribe('etls', [user], function () {
      const etls = ddp.collections.etls;
      for (let i in etls) console.log(etls[i].name);
      trace('done');
      ddp.close();
    });
  });
}

function run_remote() {
  try {
    const diff = child_process.execSync('git diff skale/master', {stdio: ['pipe', 'pipe', 'ignore']});
    if (diff.length) {
      if (argv.force) console.error('Warning, running an obsolete version, you should deploy');
      else die('Error: content has changed, deploy first or run --force');
    }
  } catch (err) {
    die('This application is not deployed. Run first "skale deploy"');
  }
  skale_session(function (err, ddp) {
    if (err) die('Could not connect:', err);
    const pkg = JSON.parse(fs.readFileSync('package.json'));
    const name = pkg.name;
    const opt = {debug: process.env.SKALE_DEBUG};
    let ltrace;

    trace('run triggered, wait for machine');
    ddp.call('etls.run', [{name: name, opt: opt}], function (err, res) {
      if (err) die('run error:', err);
      if (res.alreadyStarted) die('Error: application is already running, use "skale attach" or "skale stop"');
      ddp.subscribe('task.withTaskId', [res.taskId], function () {});

      const observer = ddp.observe('tasks');
      observer.added = function (id) {
        const task = ddp.collections.tasks[id];
        if (task.trace !== ltrace) {
          ltrace = task.trace;
          trace(ltrace);
        }
        //trace('added', ddp.collections.tasks[id]);
      };

      observer.changed = function (id, oldFields, clearedFields, newFields) {
        if (newFields.trace && newFields.trace !== ltrace) {
          ltrace = newFields.trace;
          trace(ltrace);
        }
        if (newFields.status === 'ok' || newFields.status === 'failed') {
          trace('job end', newFields.status);
          ddp.close();
        }
        if (newFields.out) {
          const olen = oldFields.out ? oldFields.out.length : 0;
          const nlen = newFields.out.length;
          for (let i = olen; i < nlen; i++) process.stdout.write(newFields.out[i] + '\n');
        }
      };
    });
  });
}

function attach() {
  skale_session(function (err, ddp) {
    if (err) die('Could not connect:', err);
    const pkg = JSON.parse(fs.readFileSync('package.json'));
    const name = pkg.name;
    ddp.subscribe('etls.withName', [name], function () {
      const etl = ddp.collections.etls[Object.keys(ddp.collections.etls)[0]];
      if (!etl.running) die('Application is not running, use "skale log" or "skale run"');

      ddp.subscribe('task.withTaskId', [etl.taskId], function () {
        const task = ddp.collections.tasks[Object.keys(ddp.collections.tasks)[0]];
        for (let i = 0; i < task.out.length; i++)
          console.log(task.out[i]);
      });

      const observer = ddp.observe('tasks');
      observer.changed = function (id, oldFields, clearedFields, newFields) {
        if (newFields.status && newFields.status != 'pending') ddp.close();
        if (newFields.out) {
          const olen = oldFields.out ? oldFields.out.length : 0;
          const nlen = newFields.out.length;
          for (let i = olen; i < nlen; i++) process.stdout.write(newFields.out[i] + '\n');
        }
      };

    });
  });
}

function log(name) {
  if (!name) {
    name = JSON.parse(fs.readFileSync('package.json')).name;
  }
  skale_session(function (err, ddp) {
    if (err) die('could not connect:', err);
    ddp.subscribe('etls.withName', [name], function () {
      const etl = ddp.collections.etls[Object.keys(ddp.collections.etls)[0]];
      ddp.subscribe('task.withTaskId', [etl.taskId], function () {
        const task = ddp.collections.tasks[Object.keys(ddp.collections.tasks)[0]];
        for (let i = 0; i < task.out.length; i++)
          console.log(task.out[i]);
        trace('done');
        ddp.close();
      });
    });
  });
}

function status(name) {
  skale_session(function (err, ddp) {
    if (err) die('could node connect:', err);
    if (!name) {
      try {
        const pkg = JSON.parse(fs.readFileSync('package.json'));
        name = pkg.name;
      } catch (err) {
        die('Could not find package.json.  You need to run this command from a skale project directory.');
      }
    }

    ddp.subscribe('etls.withName', [name], function () {
      if (!ddp.collections.etls) die('etl not found:', name);
      const etl = ddp.collections.etls[Object.keys(ddp.collections.etls)[0]];
      console.log(etl.name, 'status:', etl.running ? 'running' : 'exited');
      ddp.close();
    });
  });
}

function stop() {
  skale_session(function (err, ddp) {
    if (err) die('could node connect:', err);
    const pkg = JSON.parse(fs.readFileSync('package.json'));
    const name = pkg.name;
    ddp.call('etls.reset', [{name: name, reset: argv.force}], function () {
      ddp.close();
    });
  });
}
