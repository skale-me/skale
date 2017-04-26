// Stub node-parquet binary module.
'use strict';

try {
  module.exports = require('node-parquet');
} catch (err) {
  module.exports = { ParquetReader: stub, ParquetWriter: stub };
}

function stub() {
  throw 'Missing module, run "npm install node-parquet"';
}
