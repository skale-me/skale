#!/usr/bin/env node

const sc = require('../').context();
const ml = require('../ml');
const addAwait = require('await-outside').addAwaitOutsideToReplServer;
const repl = require('repl').start({ prompt: 'skale> ' });

addAwait(repl);
repl.context.sc = sc;
repl.context.ml = ml;
