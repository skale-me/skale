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
            t.deepEqual(res, [
              '0;0;0', '1;1;1', '2;2;2', '3;3;3', '4;4;4',
              '5;5;5', '6;6;6', '7;7;7', '8;8;8', '9;9;9'
            ], 'saved content is correct');
            sc.end();
          });
      });
  });
});
