var t = require('tape');
var sc = require('skale').context();

var data = [['hello', 1], ['hello', 1], ['world', 1]];
var nPartitions = 2;

var init = 0;

function reducer(a, b) {return a + b;}

t.test('reduceByKey', function (t) {
  t.plan(1);

  sc.parallelize(data, nPartitions)
    .reduceByKey(reducer, init)
    .collect(function(err, res) {
      t.deepEqual(res.sort(), [['hello', 2], ['world', 1]]);
      sc.end();
    });
});
