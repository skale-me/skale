#!/usr/bin/env node

const sc = require('../').context();
const addAwait = require('await-outside').addAwaitOutsideToReplServer;
const repl = require('repl').start({ prompt: 'skale> ' });

addAwait(repl);
repl.context.sc = sc;
