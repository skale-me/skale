var t = require('tape');
var sc = require('skale-engine').context();

t.test('grouByKey', function (t) {
  t.plan(1);

  sc.parallelize([['hello', 1], ['hello', 2], ['world', 1]])
    .groupByKey()
    .collect(function (err, res) {
      t.deepEqual(res.sort(), [['hello', [1, 2]], ['world', [1]]]);
      sc.end();
    });
});
