const t = require('tape');
const sc = require('skale').context();

t.test('coGroup', function (t) {
  t.plan(1);

  const data = [['hello', 1], ['world', 2], ['cedric', 3], ['cedric', 4]];
  const data2 = [['cedric', 3], ['world', 4], ['test', 5]];
  const nPartitions = 2;

  const a = sc.parallelize(data, nPartitions);
  const b = sc.parallelize(data2, nPartitions);

  a.coGroup(b).collect(function (err, res) {
    t.deepEqual(res.sort(), [
      ['cedric', [[3, 4], [3]]],
      ['hello', [[1], []]],
      ['test', [[], [5]]],
      ['world', [[2], [4]]],
    ]);
    sc.end();
  });
});
