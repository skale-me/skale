const t = require('tape');
const sc = require('skale').context();

const data = [['hello', 1], ['hello', 1], ['world', 1]];
const nPartitions = 2;

const init = 0;

function reducer(a, b) {return a + b;}
function combiner(a, b) {return a + b;}

t.test('aggregateByKey', function (t) {
  t.plan(1);

  sc.parallelize(data, nPartitions)
    .aggregateByKey(reducer, combiner, init)
    .collect(function(err, res) {
      t.deepEqual(res, [['hello', 2], ['world', 1]]);
      sc.end();
    });
});

// TODO: test passing args in combiner / reducer

// TODO: test using worker contex in combiner / reducer
