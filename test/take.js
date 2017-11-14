var t = require('tape');
var sc = require('skale').context();

t.test('first callback', function (t) {
  t.plan(1);

  sc.range(100)
    .first(function (err, res) {
      t.equal(res, 0); 
    });
});

t.test('first promise', function (t) {
  t.plan(1);

  sc.range(100)
    .first()
    .then(function (res) {
      t.equal(res, 0); 
    });
});

t.test('take callback', function (t) {
  t.plan(1);

  sc.range(100)
    .take(3, function (err, res) {
      t.deepEqual(res, [0, 1, 2]); 
    });
});

t.test('take promise', function (t) {
  t.plan(1);

  sc.range(100)
    .take(3)
    .then(function (res) {
      t.deepEqual(res, [0, 1, 2]); 
    });
});

t.test('top callback', function (t) {
  t.plan(1);

  sc.range(100)
    .top(3, function (err, res) {
      t.deepEqual(res, [97, 98, 99]); 
    });
});

t.test('top promise', function (t) {
  t.plan(1);

  sc.range(100)
    .top(3)
    .then(function (res) {
      t.deepEqual(res, [97, 98, 99]); 
      sc.end();
    });
});
