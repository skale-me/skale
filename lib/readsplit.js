// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const fs = require('fs');
//var Lines = require('../lib/lines.js');

/**
  * Compute N-Length logic split of given file
  */
function splitLocalFile(file, N, callback) {
  const split = [];
  const size = fs.statSync(file).size;
  const maxBlockSize = Math.ceil(size / (N || 1));
  let start = 0;

  // var str = fs.readFileSync(file, {encoding: 'utf8'}).replace(/\n/g, '*');
  // console.log(str)
  while (start < size) {
    // console.log('Split n° %d = %s', split.length, str.substr(start, maxBlockSize + 1))
    split.push({index: split.length, chunk: [{path: file, opt: {start: start, end: start + maxBlockSize}}]});
    start += maxBlockSize + 1;
  }

  callback(split);
}

function splitDistributedFile(file, N, callback) {  // emulates a distributed file for now
  callback([
    {
      index: 0, chunk: [
        {path: './test.dat', opt: {start: 0, end: 9}},
        {path: './test.dat', opt: {start: 10, end: 19}}
      ]
    }, {
      index: 1,
      chunk: [
        {path: './test.dat', opt: {start: 20, end: 29}},
        {path: './test.dat', opt: {start: 30}}
      ]
    }
  ]);
}

function getFirstLine(split, chunk_buffer, s, getStream, done) {
  // console.log('Split n° ' + (s - 1) + ' seeks end of line starting with : ' + chunk_buffer.replace(/\n/g, '*'))
  const isLastSplit = (split[s].index === (split.length - 1));
  let p = 0;
  let firstLineFound = false;
  let firstLine;

  function readPart(part, partDone) {
    const isLastPart = (p === split[s].chunk.length - 1);
    //const rs = fs.createReadStream(part.path, part.opt);
    const rs = getStream(part, part.opt);

    function processChunk(chunk) {
      const lines = (chunk_buffer + chunk).split(/\r\n|\r|\n/);
      chunk_buffer = lines.pop();
      if (lines.length > 0) {
        firstLine = lines[0];
        firstLineFound = true;
        //rs.destroy();
      } else rs.once('data', processChunk);
    }

    rs.once('data', processChunk);

    rs.on('end', function () {
      if (firstLineFound) done(firstLine);
      else if (!isLastPart) partDone();
      else if (isLastSplit) done(chunk_buffer);
      else {
        getFirstLine(split, chunk_buffer, s + 1, getStream, done);
      }
    });
  }

  function end() {
    if (++p < split[s].chunk.length) readPart(split[s].chunk[p], end);
  }

  readPart(split[s].chunk[p], end);
}

function readSplit(split, s, processLine, splitDone, getStream) {
  if (split.length === 0) return splitDone();
  const isFirstSplit = (split[s].index === 0);
  const isLastSplit = (split[s].index === (split.length - 1));
  let chunk_buffer = '';
  let p = 0;
  let hasToSkipFirstLine = isFirstSplit ? false : undefined;
  let firstLineFound = isFirstSplit ? true : false;

  function readPart(part, partDone) {
    const isFirstPart = (p === 0);
    const isLastPart = (p === split[s].chunk.length - 1);
    const opt = (!isFirstSplit && isFirstPart) ? {start: part.opt.start - 1, end: part.opt.end} : part.opt;
    //const rs = fs.createReadStream(part.path, opt);
    const rs = getStream(part, opt);
    let chunkLastChar = '';

    function processChunkOnce(chunk) {
      // console.log('Split n° %d found chunk = %s', s, String(chunk).replace(/\n/g, '*'))
      if (hasToSkipFirstLine === undefined) {
        chunk = String(chunk);
        hasToSkipFirstLine = (chunk.charAt(0) !== '\n');
        // console.log('Has to skip first line = ' + hasToSkipFirstLine)
        chunk = chunk.substr(1);
        // console.log('Chunk after first byte test = ' + chunk)
        if (!hasToSkipFirstLine) firstLineFound = true;
      }
      const str = (chunk_buffer + chunk);
      chunkLastChar = str.charAt(str.length - 1);
      const lines = str.split(/\r\n|\r|\n/);
      chunk_buffer = lines.pop();
      if (lines.length) {
        firstLineFound = true;
        const start = hasToSkipFirstLine ? 1 : 0;
        for (let i = start; i < lines.length; i++) processLine(lines[i]);
        if (lines.length === 1) chunkLastChar = '';
        rs.on('data', processChunk);
        // console.log('Found first line')
      } else rs.once('data', processChunkOnce);
    }

    const processChunk = function(chunk) {
      const str = chunk_buffer + chunk;
      chunkLastChar = str.charAt(str.length - 1);
      const lines = str.split(/\r\n|\r|\n/);
      chunk_buffer = lines.pop();
      for (let i = 0; i < lines.length; ++i) processLine(lines[i]);
    };

    rs.on('end', function () {
      // console.log(chunk_buffer)
      if (!isLastPart) return partDone();
      if (isLastSplit) {
        if (!firstLineFound) {
          firstLineFound = true;
          if (!hasToSkipFirstLine) processLine(chunk_buffer);
        } else processLine(chunk_buffer);
        splitDone();
      } else {
        if (!firstLineFound) {
          if (chunkLastChar === '\n') {
            firstLineFound = true;
            if (!hasToSkipFirstLine) processLine(chunk_buffer);
          }
          splitDone();
        } else {
          if (chunkLastChar === '\n') {
            processLine(chunk_buffer);
            splitDone();
          } else {
            if (chunk_buffer === '') {
              splitDone();
            } else {
              getFirstLine(split, chunk_buffer, s + 1, getStream, function(firstline) {
                processLine(firstline);
                splitDone();
              });
            }
          }
        }
      }
    });

    rs.once('data', processChunkOnce);
  }

  function end() {
    if (++p < split[s].chunk.length)
      readPart(split[s].chunk[p], end);
  }

  readPart(split[s].chunk[p], end);
}

module.exports.splitLocalFile = splitLocalFile;
module.exports.splitDistributedFile = splitDistributedFile;
module.exports.readSplit = readSplit;
