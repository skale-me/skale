const t = require('tape');
const sc = require('skale').context();

t.test('intersection', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    .intersection(sc.parallelize([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]))
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [5, 6, 7, 8, 9]);    
      sc.end();
    });
});
