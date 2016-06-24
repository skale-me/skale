# Change Log

## [0.6.0](https://github.com/skale-me/skale-engine/tree/0.6.0) (2016-06-24)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.5.3...0.6.0)

**Merged pull requests:**

- stop testing node-0.10 [\#69](https://github.com/skale-me/skale-engine/pull/69) ([mvertes](https://github.com/mvertes))
- add new source gzipFile\(\) to process gzipped text files [\#68](https://github.com/skale-me/skale-engine/pull/68) ([mvertes](https://github.com/mvertes))
- New action Dataset\#save\(\) to save dataset content in text file. [\#67](https://github.com/skale-me/skale-engine/pull/67) ([mvertes](https://github.com/mvertes))
- Pass user options and worker context to reduce\(\), aggregate\(\) and forEach\(\) callbacks. [\#66](https://github.com/skale-me/skale-engine/pull/66) ([mvertes](https://github.com/mvertes))
- Serverless version, direct IPCs between workers and master [\#65](https://github.com/skale-me/skale-engine/pull/65) ([mvertes](https://github.com/mvertes))
- code cleaning [\#64](https://github.com/skale-me/skale-engine/pull/64) ([mvertes](https://github.com/mvertes))
- improve tmp dir handling. Add env SKALE\_TMP [\#63](https://github.com/skale-me/skale-engine/pull/63) ([mvertes](https://github.com/mvertes))
- remove spurious test file [\#62](https://github.com/skale-me/skale-engine/pull/62) ([mvertes](https://github.com/mvertes))

## [0.5.3](https://github.com/skale-me/skale-engine/tree/0.5.3) (2016-05-17)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.5.2...0.5.3)

**Closed issues:**

- skale-engine can not be used as a dependency in a Meteor project [\#56](https://github.com/skale-me/skale-engine/issues/56)
- Get rid of direct eval [\#53](https://github.com/skale-me/skale-engine/issues/53)
- Use mkdirp.sync instead of lib/mkdir.js [\#51](https://github.com/skale-me/skale-engine/issues/51)

**Merged pull requests:**

- rename lib/sizeof.js in lib/rough-sizeof.js as discussed in \#52 [\#60](https://github.com/skale-me/skale-engine/pull/60) ([mvertes](https://github.com/mvertes))
- Use indirect eval, strenghten regexp which match arrow function definition; fix \#53 [\#59](https://github.com/skale-me/skale-engine/pull/59) ([mvertes](https://github.com/mvertes))
- remove lib/mkdir.js and use external mkdirp module. Fix \#51 [\#58](https://github.com/skale-me/skale-engine/pull/58) ([mvertes](https://github.com/mvertes))
- Ensure compatibility with node back to 0.10, fix \#56 [\#57](https://github.com/skale-me/skale-engine/pull/57) ([mvertes](https://github.com/mvertes))
- better use of console.log\(\) [\#50](https://github.com/skale-me/skale-engine/pull/50) ([mvertes](https://github.com/mvertes))
- ds.filter\(\): rename internal filter member into \_filter to avoid coll… [\#49](https://github.com/skale-me/skale-engine/pull/49) ([mvertes](https://github.com/mvertes))

## [0.5.2](https://github.com/skale-me/skale-engine/tree/0.5.2) (2016-05-04)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.5.1...0.5.2)

**Merged pull requests:**

- Fix previous changes incompatible with node4-LTS [\#48](https://github.com/skale-me/skale-engine/pull/48) ([mvertes](https://github.com/mvertes))

## [0.5.1](https://github.com/skale-me/skale-engine/tree/0.5.1) (2016-05-04)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.5.0...0.5.1)

**Merged pull requests:**

- doc: fix doc on actions [\#47](https://github.com/skale-me/skale-engine/pull/47) ([mvertes](https://github.com/mvertes))
- fix ds.first\(\) to return an element instead of an array [\#46](https://github.com/skale-me/skale-engine/pull/46) ([mvertes](https://github.com/mvertes))
- remove lib/ml.js, restructure examples [\#45](https://github.com/skale-me/skale-engine/pull/45) ([mvertes](https://github.com/mvertes))
- various fixes [\#44](https://github.com/skale-me/skale-engine/pull/44) ([mvertes](https://github.com/mvertes))

## [0.5.0](https://github.com/skale-me/skale-engine/tree/0.5.0) (2016-05-02)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.5...0.5.0)

**Merged pull requests:**

- benchmark: update to new action syntax [\#43](https://github.com/skale-me/skale-engine/pull/43) ([mvertes](https://github.com/mvertes))
- Remove toArray\(\). Fix examples [\#42](https://github.com/skale-me/skale-engine/pull/42) ([mvertes](https://github.com/mvertes))
- All actions take an optional callback and return promises instead of … [\#41](https://github.com/skale-me/skale-engine/pull/41) ([mvertes](https://github.com/mvertes))
- simplify JS benchmark code to make it almost identical to python version [\#40](https://github.com/skale-me/skale-engine/pull/40) ([mvertes](https://github.com/mvertes))
- add benchmark [\#38](https://github.com/skale-me/skale-engine/pull/38) ([mvertes](https://github.com/mvertes))

## [0.4.5](https://github.com/skale-me/skale-engine/tree/0.4.5) (2016-04-25)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.4...0.4.5)

**Merged pull requests:**

- Provide a faster sizeof, without external dependency [\#37](https://github.com/skale-me/skale-engine/pull/37) ([mvertes](https://github.com/mvertes))
- code cleaning [\#36](https://github.com/skale-me/skale-engine/pull/36) ([mvertes](https://github.com/mvertes))
- fix broken arrow function example [\#35](https://github.com/skale-me/skale-engine/pull/35) ([mvertes](https://github.com/mvertes))
- Command line option to set max  memory per worker [\#34](https://github.com/skale-me/skale-engine/pull/34) ([mvertes](https://github.com/mvertes))
- master: set process title to ease monitoring [\#33](https://github.com/skale-me/skale-engine/pull/33) ([mvertes](https://github.com/mvertes))

## [0.4.4](https://github.com/skale-me/skale-engine/tree/0.4.4) (2016-04-20)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.3...0.4.4)

**Merged pull requests:**

- worker: fix init of Partition.mm [\#32](https://github.com/skale-me/skale-engine/pull/32) ([mvertes](https://github.com/mvertes))

## [0.4.3](https://github.com/skale-me/skale-engine/tree/0.4.3) (2016-04-20)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.2...0.4.3)

**Merged pull requests:**

- doc: various fixes [\#31](https://github.com/skale-me/skale-engine/pull/31) ([mvertes](https://github.com/mvertes))
- Dataset: provide a better RNG. Expose Random and Poisson classes. [\#30](https://github.com/skale-me/skale-engine/pull/30) ([mvertes](https://github.com/mvertes))
- On workers: evict a partition if going out of memory [\#29](https://github.com/skale-me/skale-engine/pull/29) ([mvertes](https://github.com/mvertes))
- Handle connection error to terminate a master if a worker fails [\#28](https://github.com/skale-me/skale-engine/pull/28) ([mvertes](https://github.com/mvertes))

## [0.4.2](https://github.com/skale-me/skale-engine/tree/0.4.2) (2016-04-17)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.1...0.4.2)

**Merged pull requests:**

- Preliminary work to isolate machine learning capabilities from core engine [\#27](https://github.com/skale-me/skale-engine/pull/27) ([CedricArtigue](https://github.com/CedricArtigue))
- fix lint [\#26](https://github.com/skale-me/skale-engine/pull/26) ([mvertes](https://github.com/mvertes))
- run eslint in pre-test [\#25](https://github.com/skale-me/skale-engine/pull/25) ([mvertes](https://github.com/mvertes))
- fix lint problems. Remove dead code [\#24](https://github.com/skale-me/skale-engine/pull/24) ([mvertes](https://github.com/mvertes))
- new lint fixes [\#23](https://github.com/skale-me/skale-engine/pull/23) ([mvertes](https://github.com/mvertes))
- Simplify code, fix lint errors. More to come. [\#22](https://github.com/skale-me/skale-engine/pull/22) ([mvertes](https://github.com/mvertes))
- add eslint. Start code cleaning [\#21](https://github.com/skale-me/skale-engine/pull/21) ([mvertes](https://github.com/mvertes))

## [0.4.1](https://github.com/skale-me/skale-engine/tree/0.4.1) (2016-04-07)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.4.0...0.4.1)

**Merged pull requests:**

- document partitioners [\#20](https://github.com/skale-me/skale-engine/pull/20) ([mvertes](https://github.com/mvertes))
- doc: fix cross references [\#19](https://github.com/skale-me/skale-engine/pull/19) ([mvertes](https://github.com/mvertes))
- document ds.top\(\) [\#18](https://github.com/skale-me/skale-engine/pull/18) ([mvertes](https://github.com/mvertes))
- document ds.take\(\) [\#17](https://github.com/skale-me/skale-engine/pull/17) ([mvertes](https://github.com/mvertes))
- document ds.sortByKey\(\) [\#16](https://github.com/skale-me/skale-engine/pull/16) ([mvertes](https://github.com/mvertes))
- document ds.sortBy\(\) [\#15](https://github.com/skale-me/skale-engine/pull/15) ([mvertes](https://github.com/mvertes))
- document ds.persist\(\) [\#14](https://github.com/skale-me/skale-engine/pull/14) ([mvertes](https://github.com/mvertes))
- Document ds.partitionBy\(\) and fix example [\#13](https://github.com/skale-me/skale-engine/pull/13) ([mvertes](https://github.com/mvertes))
- document ds.first\(\) [\#12](https://github.com/skale-me/skale-engine/pull/12) ([mvertes](https://github.com/mvertes))
- document ds.aggregateByKey\(\) [\#11](https://github.com/skale-me/skale-engine/pull/11) ([mvertes](https://github.com/mvertes))
- aggregateByKey: change args order to match aggregate [\#10](https://github.com/skale-me/skale-engine/pull/10) ([mvertes](https://github.com/mvertes))
- document sc.range\(\) [\#9](https://github.com/skale-me/skale-engine/pull/9) ([mvertes](https://github.com/mvertes))
- use sizeof from external npm dependency [\#8](https://github.com/skale-me/skale-engine/pull/8) ([mvertes](https://github.com/mvertes))
- rename RDD in dataset. No functional change. [\#7](https://github.com/skale-me/skale-engine/pull/7) ([mvertes](https://github.com/mvertes))

## [0.4.0](https://github.com/skale-me/skale-engine/tree/0.4.0) (2016-04-04)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.3.5...0.4.0)

**Merged pull requests:**

- All actions now return a readable stream [\#6](https://github.com/skale-me/skale-engine/pull/6) ([mvertes](https://github.com/mvertes))

## [0.3.5](https://github.com/skale-me/skale-engine/tree/0.3.5) (2016-04-03)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.3.4...0.3.5)

**Fixed bugs:**

- cartesian incorrect output [\#4](https://github.com/skale-me/skale-engine/issues/4)

**Merged pull requests:**

- lib/dataset: cartesian.iterate\(\): fix partition index computation. Fix \#4 [\#5](https://github.com/skale-me/skale-engine/pull/5) ([mvertes](https://github.com/mvertes))
- .travis.yml: set a fixed number of workers \(4\)  [\#3](https://github.com/skale-me/skale-engine/pull/3) ([mvertes](https://github.com/mvertes))

## [0.3.4](https://github.com/skale-me/skale-engine/tree/0.3.4) (2016-04-01)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.3.3...0.3.4)

**Fixed bugs:**

- examples/core/parallelize.js fails with 2 workers [\#2](https://github.com/skale-me/skale-engine/issues/2)

## [0.3.3](https://github.com/skale-me/skale-engine/tree/0.3.3) (2016-03-25)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.3.2...0.3.3)

## [0.3.2](https://github.com/skale-me/skale-engine/tree/0.3.2) (2016-03-23)
**Merged pull requests:**

- Add a Gitter chat badge to README.md [\#1](https://github.com/skale-me/skale-engine/pull/1) ([gitter-badger](https://github.com/gitter-badger))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*