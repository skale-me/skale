const t = require('tape');
const sc = require('skale').context();

t.test('textFile local file', function (t) {
  t.plan(1);
  sc.textFile(__dirname + '/data/iris.csv')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile compressed file', function (t) {
  t.plan(1);
  sc.textFile(__dirname + '/data/iris.csv.gz')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile dir', function (t) {
  t.plan(1);
  sc.textFile(__dirname + '/data/split/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile compressed dir', function (t) {
  t.plan(1);
  sc.textFile(__dirname + '/data/split-gz/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile multiple files', function (t) {
  t.plan(1);
  sc.textFile(__dirname + '/data/split/iris-*.csv')
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});

