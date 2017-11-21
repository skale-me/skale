const t = require('tape');
const rimraf = require('rimraf');
const sc = require('skale').context();

const savedir = '/tmp/skale-test/save';

t.test('save', function (t) {
  t.plan(1);

  rimraf.sync(savedir);
  sc.range(10)
    .save('/tmp/skale-test/save', function (err, res) {
      sc.textFile(savedir + '/')
        .map(a => JSON.parse(a))
        .collect(function (err, res) {
          t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
          sc.end();
        });
    });
});
