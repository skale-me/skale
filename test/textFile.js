const t = require('tape');
const sc = require('skale').context();

const file = __dirname + '/data/iris.csv';

t.test('textFile', function (t) {
  t.plan(1);

  sc.textFile(file)
    .count(function (err, res) {
      t.ok(res === 151);
      sc.end();
    });
});

