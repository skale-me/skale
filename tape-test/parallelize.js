var t = require('tape');
var sc = require('skale-engine').context();

t.onFinish(sc.end);

t.test('parallelize', function (t) {
  sc.parallelize([0, 1, 2, 3]).collect(function (err, data) {
    t.deepEqual(data, [0, 1, 2, 3]);
    t.end();
  });
});
