var t = require('tape');
var sc = require('skale-engine').context();

var data = [['world', 2], ['cedric', 3], ['hello', 1]];
var nPartitions = 2;

t.test('sortByKey', function (t) {
  t.plan(1);

  sc.parallelize(data, nPartitions)
    .sortByKey()
    .collect(function(err, res) {
      t.deepEqual(res, [['cedric', 3], ['hello', 1], ['world', 2]]);
      sc.end();
    });
});
