const t = require('tape');
const sc = require('skale').context();

t.test('distinct', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 1, 4, 3, 5])
    .distinct()
    .collect(function (err, res) {
      t.deepEqual(res.sort(), [1, 2, 3, 4, 5]);
      sc.end();
    });
});

