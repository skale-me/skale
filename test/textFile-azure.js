const t = require('tape');
const sc = require('skale').context();

const skip = process.env.AZURE_STORAGE_CONNECTION_STRING ? false : true;

t.test('textFile azure file', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('wasb://skalejs/test/iris.csv')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure compressed file', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('wasb://skalejs/test/iris.csv.gz')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure dir', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('wasb://skalejs/split/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure compressed dir', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('wasb://skalejs/splitgz/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure multiple files', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('wasb://skalejs/split/iris-*.csv')
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});

if (skip) sc.end();
