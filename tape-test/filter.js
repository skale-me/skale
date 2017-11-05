var t = require('tape');
var sc = require('skale-engine').context();

t.test('filter', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 4])
    .filter(a => a % 2)
    .collect(function (err, res) {
      t.deepEqual(res, [1, 3]); 
      sc.end();
    });
});
