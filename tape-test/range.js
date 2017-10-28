var t = require('tape');
var sc = require('skale-engine').context();

t.onFinish(sc.end);

t.test('range', function (t) {
  t.plan(3);
  sc.range(4).collect(function (err, data) {
    t.deepEqual(data, [0, 1, 2, 3]);
  });
  sc.range(2, 4).collect(function (err, data) {
    t.deepEqual(data, [2, 3]);
  });
  sc.range(10, -5, -3).collect(function (err, data) {
    t.deepEqual(data, [10, 7, 4, 1, -2]);
  });
});
