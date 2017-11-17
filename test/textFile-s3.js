const t = require('tape');
const sc = require('skale').context();

console.log('AWS_ACCESS_KEY_ID:', process.env.AWS_ACCESS_KEY_ID);

t.test('textFile s3 file', function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/iris.csv')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 compressed file', function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/iris.csv.gz')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 dir', function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 compressed dir', function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split-gz/')
    .count(function (err, res) {
      t.ok(res === 151);
    });
});

t.test('textFile s3 multiple files', function (t) {
  t.plan(1);
  sc.textFile('s3://skale-test-eu-west-1/test/split/iris-*.csv')
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});
