var t = require('tape');
var sc = require('skale-engine').context();

t.onFinish(sc.end);

t.test('aggregate', function (t) {
  t.plan(2);

  sc.parallelize([3, 5, 2, 7, 4, 8])
    .aggregate(
      (a, v) => [a[0] + v, a[1] + 1],
      (a1, a2) => [a1[0] + a2[0], a1[1] + a2[1]],
      [0, 0],
      function (err, res) {
        t.equal(res[0] / res[1], 29 / 6);
      }
    );

  sc.parallelize([3, 5, 2, 7, 4, 8])
    .aggregate(
      (a, v) => [a[0] + v, a[1] + 1],
      (a1, a2) => [a1[0] + a2[0], a1[1] + a2[1]],
      [0, 0]
    )
    .then(function(res) {
      t.equal(res[0] / res[1], 29 / 6);
    });
});
