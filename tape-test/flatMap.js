var t = require('tape');
var sc = require('skale-engine').context();

t.test('flatMap', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 4])
    .flatMap(a => [a, a])
    .collect(function (err, res) {
      t.deepEqual(res, [1, 1, 2, 2, 3, 3, 4, 4]);
    });
});

t.test('flatMapValues', function (t) {
  t.plan(1);

  sc.parallelize([['hello', 1], ['world', 2]])
    .flatMapValues(a => [a, 2 * a])
    .collect(function (err, res) {
      t.deepEqual(res, [
        ['hello', 1], ['hello', 2],
        ['world', 2], ['world', 4]
      ]);
      sc.end();
    });
});
