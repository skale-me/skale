var t = require('tape');
var sc = require('skale-engine').context();

var data = [['hello', 1], ['world', 2], ['world', 3]];
var data2 = [['cedric', 3], ['world', 4]];

t.test('join', function (t) {
  t.plan(1);

  sc.parallelize(data)
    .join(sc.parallelize(data2))
    .collect(function(err, res) {
      t.deepEqual(res, [['world', [2, 4]], ['world', [3, 4]]]);
    });
});

t.test('leftOuterJoin', function (t) {
  t.plan(1);

  sc.parallelize(data)
    .leftOuterJoin(sc.parallelize(data2))
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [['hello', [1, null]], ['world', [2, 4]], ['world', [3, 4]]]);
    });
});

t.test('rightOuterJoin', function (t) {
  t.plan(1);

  sc.parallelize(data)
    .rightOuterJoin(sc.parallelize(data2))
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [['cedric', [null, 3]], ['world', [2, 4]], ['world', [3, 4]]]);
      sc.end();
    });
});
