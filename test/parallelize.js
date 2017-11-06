var t = require('tape');
var sc = require('skale-engine').context();

t.test('parallelize', function (t) {
  t.plan(1);

  sc.parallelize([0, 1, 2, 3]).collect(function (err, data) {
    t.deepEqual(data, [0, 1, 2, 3]);
    sc.end();
  });
});
