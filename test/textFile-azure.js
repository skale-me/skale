const t = require('tape');
const sc = require('skale').context();

const skip = process.env.AZURE_STORAGE_CONNECTION_STRING ? false : true;

t.test('textFile azure file', function (t) {
  t.plan(1);
  if (skip) return t.pass('SKIP # no Azure storage key');
  sc.textFile('wasb://test/iris.csv')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure compressed file', function (t) {
  t.plan(1);
  if (skip) return t.pass('SKIP # no Azure storage key');
  sc.textFile('wasb://test/iris.csv.gz')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure dir', function (t) {
  t.plan(1);
  if (skip) return t.pass('SKIP # no Azure storage key');
  sc.textFile('wasb://split/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure compressed dir', function (t) {
  t.plan(1);
  if (skip) return t.pass('SKIP # no Azure storage key');
  sc.textFile('wasb://splitgz/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile azure multiple files', function (t) {
  t.plan(1);
  if (skip) {
    sc.end();
    return t.pass('SKIP # no Azure storage key');
  }
  sc.textFile('wasb://split/iris-*.csv')
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});
