const t = require('tape');
const sc = require('skale').context();

t.test('union', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 4])
    .union(sc.parallelize([5, 6, 7, 8]))
    .collect(function (err, res) {
      t.deepEqual(res, [1, 2, 3, 4, 5, 6, 7, 8]);
      sc.end();
    });
});
