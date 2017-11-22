const t = require('tape');
const sc = require('skale').context();

const data = [['world', 2], ['cedric', 3], ['hello', 1]];
const nPartitions = 2;

t.test('sortByKey', function (t) {
  t.plan(1);

  sc.parallelize(data, nPartitions)
    .sortByKey()
    .collect(function(err, res) {
      t.deepEqual(res, [['cedric', 3], ['hello', 1], ['world', 2]]);
      sc.end();
    });
});
