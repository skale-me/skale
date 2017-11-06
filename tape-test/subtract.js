var t = require('tape');
var sc = require('skale-engine').context();

var d1 = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];
var d2 = [[1, 1], [1, 1], [2, 3]];

t.test('subtract', function (t) {
  t.plan(1);

  sc.parallelize(d1)
    .subtract(sc.parallelize(d2))
    .collect(function(err, res) {
      console.log(res);
      t.deepEqual(res.sort(), [[2, 4], [3, 5]]);
      sc.end();
    });
});
