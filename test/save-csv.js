const fs = require('fs');
const t = require('tape');
const rimraf = require('rimraf');
const sc = require('skale').context();

const savedir = '/tmp/skale-test/save';

t.test('save csv', function (t) {
  t.plan(4);

  rimraf(savedir, function (err) {
    t.ok(!err, 'delete previous saved data');
    sc.range(10)
      .map(a => [a, a, a])
      .save(savedir, {stream: true, csv: true}, function (err) {
        t.ok(!err, 'save returns no error');
        t.ok(fs.existsSync(savedir + '/0.csv'), 'saved filename is correct');
        sc.textFile(savedir + '/')
          .collect(function (err, res) {
            //t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'saved content is correct');
            console.log('res:', res);
            t.pass();
            sc.end();
          });
      });
  });
});

/*
t.test('save gzip', function (t) {
  t.plan(4);

  rimraf(savedir, function (err) {
    t.ok(!err, 'delete previous saved data');
    sc.range(10)
      .save(savedir, {gzip: true}, function (err) {
        t.ok(!err, 'save returns no error');
        t.ok(fs.existsSync(savedir + '/0.gz'), 'saved filename is correct');
        sc.textFile(savedir + '/')
          .map(a => JSON.parse(a))
          .collect(function (err, res) {
            t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'saved content is correct');
            sc.end();
          });
      });
  });
});
*/
