const t = require('tape');
const sc = require('skale').context();

const skip = process.env.AWS_ACCESS_KEY_ID ? false : true;

t.test('textFile s3 file', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/iris.csv')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 compressed file', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/iris.csv.gz')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 dir', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 compressed dir', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split-gz/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 multiple files', {skip: skip}, function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split/iris-*.csv')
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});

if (skip) sc.end();
