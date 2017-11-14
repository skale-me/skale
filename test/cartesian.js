var t = require('tape');
var sc = require('skale').context();

t.test('cartesian', function (t) {
  var data = [1, 2, 3, 4, 5, 6];
  var data2 = [7, 8, 9, 10, 11, 12];
  var nPartitions = 3;

  var a = sc.parallelize(data, nPartitions);
  var b = sc.parallelize(data2, nPartitions);

  t.plan(1);

  a.cartesian(b)
    .collect(function(err, res) {
      res.sort();
      t.deepEqual(res, [
        [1, 10], [1, 11], [1, 12], [1, 7], [1, 8], [1, 9],
        [2, 10], [2, 11], [2, 12], [2, 7], [2, 8], [2, 9],
        [3, 10], [3, 11], [3, 12], [3, 7], [3, 8], [3, 9],
        [4, 10], [4, 11], [4, 12], [4, 7], [4, 8], [4, 9],
        [5, 10], [5, 11], [5, 12], [5, 7], [5, 8], [5, 9],
        [6, 10], [6, 11], [6, 12], [6, 7], [6, 8], [6, 9]
      ]);
      sc.end();
    });
});
