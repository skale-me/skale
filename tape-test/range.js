var t = require('tape');
var sc = require('skale-engine').context();

t.test('range 1 arg', function (t) {
  t.plan(1);
  sc.range(4).collect(function (err, data) {
    t.deepEqual(data, [0, 1, 2, 3]);
  });
});

t.test('range 2 args', function (t) {
  t.plan(1);
  sc.range(2, 4).collect(function (err, data) {
    t.deepEqual(data, [2, 3]);
  });
});

t.test('range 3 args', function (t) {
  t.plan(1);
  sc.range(10, -5, -3).collect(function (err, data) {
    t.deepEqual(data, [10, 7, 4, 1, -2]);
    sc.end();
  });
});
