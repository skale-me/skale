var t = require('tape');
var sc = require('skale-engine').context();

t.test('keys', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [2, 4], [4, 6]])
    .keys()
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [1, 2, 4]);
    });
});

t.test('values', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [2, 4], [4, 6]])
    .values()
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [2, 4, 6]);
      sc.end();
    });
});
