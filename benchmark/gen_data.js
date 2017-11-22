#!/usr/bin/env node

const fs = require('fs');

if (process.argv.length !== 4) {
  console.log('Usage: gen_data.js file size_in_Mo');
  process.exit(1);
}

const file = process.argv[2];
const D = 16;
const maxSize = process.argv[3] * 1024 * 1024;

const rng = new Random();
const fd = fs.createWriteStream(file);
let fileSize = 0;

function writeChunk() {
  let line = '';
  for (let i = 0; i < 500; i++) {
    line += 2 * Math.round(Math.abs(rng.randn(1))) - 1;
    line += ' ' + rng.randn(D).join(' ') + '\n';
  }
  const lineSize = Buffer.byteLength(line, 'utf8');
  if ((fileSize + lineSize) > maxSize) fd.end();
  else fd.write(line, function() {fileSize += lineSize; writeChunk();});
}

writeChunk();

function Random(initSeed) {
  this.seed = initSeed || 1;

  this.next = function () {
    const x = Math.sin(this.seed++) * 10000;
    return (x - Math.floor(x)) * 2 - 1;
  };

  this.reset = function () {
    this.seed = initSeed;
  };

  this.randn = function (N) {
    const w = new Array(N);
    for (let i = 0; i < N; i++)
      w[i] = this.next();
    return w;
  };
}
