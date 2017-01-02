# Change Log

## [0.6.9](https://github.com/skale-me/skale-engine/tree/0.6.9) (2017-01-02)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.8...0.6.9)

**Merged pull requests:**

- textFile: add a maxFiles option, to limit the number of files to process [\#117](https://github.com/skale-me/skale-engine/pull/117) ([mvertes](https://github.com/mvertes))
- Increase number of streams over skale protocol [\#116](https://github.com/skale-me/skale-engine/pull/116) ([mvertes](https://github.com/mvertes))
- stream\(\) action: add option to terminate context on stream end [\#115](https://github.com/skale-me/skale-engine/pull/115) ([mvertes](https://github.com/mvertes))
- doc: Fix TOC link [\#114](https://github.com/skale-me/skale-engine/pull/114) ([mvertes](https://github.com/mvertes))

## [0.6.8](https://github.com/skale-me/skale-engine/tree/0.6.8) (2016-12-14)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.7...0.6.8)

**Fixed bugs:**

- In distributed mode, temporary files are not deleted at end of program [\#110](https://github.com/skale-me/skale-engine/issues/110)

**Merged pull requests:**

- Advertise benchmark in README [\#113](https://github.com/skale-me/skale-engine/pull/113) ([mvertes](https://github.com/mvertes))
- server, worker-controller: add current statistics [\#112](https://github.com/skale-me/skale-engine/pull/112) ([mvertes](https://github.com/mvertes))
- Fix bug \#110 where temporary files were not deleted at end of task in… [\#111](https://github.com/skale-me/skale-engine/pull/111) ([mvertes](https://github.com/mvertes))
- use SKALE\_WORKERS to set number of workers in distributed mode, as in… [\#109](https://github.com/skale-me/skale-engine/pull/109) ([mvertes](https://github.com/mvertes))
- update dependencies [\#108](https://github.com/skale-me/skale-engine/pull/108) ([mvertes](https://github.com/mvertes))
- Doc: add a section on core concepts, label shuffle transforms [\#107](https://github.com/skale-me/skale-engine/pull/107) ([mvertes](https://github.com/mvertes))
- Refactor internal Task API, to propagate env and dependencies. In pro… [\#106](https://github.com/skale-me/skale-engine/pull/106) ([mvertes](https://github.com/mvertes))

## [0.6.7](https://github.com/skale-me/skale-engine/tree/0.6.7) (2016-11-22)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.6...0.6.7)

**Merged pull requests:**

- Document standalone and distributed modes [\#105](https://github.com/skale-me/skale-engine/pull/105) ([mvertes](https://github.com/mvertes))
- Distributed mode: Implement peer-to-peer through HTTP for shuffle data transfer [\#104](https://github.com/skale-me/skale-engine/pull/104) ([mvertes](https://github.com/mvertes))
- Improve traces and file transfer. [\#103](https://github.com/skale-me/skale-engine/pull/103) ([mvertes](https://github.com/mvertes))
- Improve task scheduling [\#102](https://github.com/skale-me/skale-engine/pull/102) ([mvertes](https://github.com/mvertes))
- Fix mocha dependency. Skip yarn files. [\#101](https://github.com/skale-me/skale-engine/pull/101) ([mvertes](https://github.com/mvertes))

## [0.6.6](https://github.com/skale-me/skale-engine/tree/0.6.6) (2016-11-04)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.5...0.6.6)

**Merged pull requests:**

- Task serialization is now correct in all cases. [\#100](https://github.com/skale-me/skale-engine/pull/100) ([mvertes](https://github.com/mvertes))
- Optimize task data transfer by sending sparse datasets. [\#99](https://github.com/skale-me/skale-engine/pull/99) ([mvertes](https://github.com/mvertes))
- Preliminary work for optimization of task data transfer [\#98](https://github.com/skale-me/skale-engine/pull/98) ([mvertes](https://github.com/mvertes))
- improve getReadStream [\#97](https://github.com/skale-me/skale-engine/pull/97) ([mvertes](https://github.com/mvertes))
- distributed mode: fixes in compressed transfers [\#96](https://github.com/skale-me/skale-engine/pull/96) ([mvertes](https://github.com/mvertes))
- Improve distributed mode [\#95](https://github.com/skale-me/skale-engine/pull/95) ([mvertes](https://github.com/mvertes))
- distributed mode: protocol debug traces enabled with SKALE\_DEBUG=3 [\#94](https://github.com/skale-me/skale-engine/pull/94) ([mvertes](https://github.com/mvertes))
- sc.textFile: fix handling of S3 directories [\#93](https://github.com/skale-me/skale-engine/pull/93) ([mvertes](https://github.com/mvertes))
- Fix log in distributed worker [\#92](https://github.com/skale-me/skale-engine/pull/92) ([mvertes](https://github.com/mvertes))

## [0.6.5](https://github.com/skale-me/skale-engine/tree/0.6.5) (2016-10-23)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.4...0.6.5)

**Merged pull requests:**

- bin/worker.js: fix typo in dependencies [\#91](https://github.com/skale-me/skale-engine/pull/91) ([mvertes](https://github.com/mvertes))
- Debug traces sent to stderr instead of stdout [\#90](https://github.com/skale-me/skale-engine/pull/90) ([mvertes](https://github.com/mvertes))
- save to S3: extend http timeout to 1h [\#89](https://github.com/skale-me/skale-engine/pull/89) ([mvertes](https://github.com/mvertes))

## [0.6.4](https://github.com/skale-me/skale-engine/tree/0.6.4) (2016-10-11)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.3...0.6.4)

**Merged pull requests:**

- dataset stream and save: preserve structure [\#88](https://github.com/skale-me/skale-engine/pull/88) ([mvertes](https://github.com/mvertes))
- textFile: add capability to handle single AWS S3 files, gzipped or not. [\#87](https://github.com/skale-me/skale-engine/pull/87) ([mvertes](https://github.com/mvertes))

## [0.6.3](https://github.com/skale-me/skale-engine/tree/0.6.3) (2016-10-08)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.2...0.6.3)

**Merged pull requests:**

- Add new stream action, which allows to stream out a dataset [\#86](https://github.com/skale-me/skale-engine/pull/86) ([mvertes](https://github.com/mvertes))

## [0.6.2](https://github.com/skale-me/skale-engine/tree/0.6.2) (2016-09-14)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.1...0.6.2)

**Closed issues:**

- bin/server doesn't use nworker parameter [\#82](https://github.com/skale-me/skale-engine/issues/82)

**Merged pull requests:**

- Fix save\(\) action to export a dataset to filesystem or S3 [\#84](https://github.com/skale-me/skale-engine/pull/84) ([mvertes](https://github.com/mvertes))
- Remove unused nworker command line parameter [\#83](https://github.com/skale-me/skale-engine/pull/83) ([mark-bradshaw](https://github.com/mark-bradshaw))
- Minor grammar update. [\#80](https://github.com/skale-me/skale-engine/pull/80) ([mark-bradshaw](https://github.com/mark-bradshaw))
- Add streaming source from AWS S3 [\#78](https://github.com/skale-me/skale-engine/pull/78) ([mvertes](https://github.com/mvertes))
- local worker: handle master disconnect [\#77](https://github.com/skale-me/skale-engine/pull/77) ([mvertes](https://github.com/mvertes))
- Increase buffer size of zlib from 16kB to 64kB. Better performances [\#76](https://github.com/skale-me/skale-engine/pull/76) ([mvertes](https://github.com/mvertes))
- textFile now supports directory as dataset source [\#75](https://github.com/skale-me/skale-engine/pull/75) ([mvertes](https://github.com/mvertes))
- Coding style change. We now use the same as NodeJS core. [\#74](https://github.com/skale-me/skale-engine/pull/74) ([mvertes](https://github.com/mvertes))

## [0.6.1](https://github.com/skale-me/skale-engine/tree/0.6.1) (2016-07-05)
[Full Changelog](https://github.com/skale-me/skale-engine/compare/0.6.0...0.6.1)

**Merged pull requests:**

- Important improvements in task serialization, scheduling and debugging [\#73](https://github.com/skale-me/skale-engine/pull/73) ([mvertes](https://github.com/mvertes))
- save: reset file at init [\#72](https://github.com/skale-me/skale-engine/pull/72) ([mvertes](https://github.com/mvertes))
- Add SKALE\_MEMORY env variable to set worker max memory [\#71](https://github.com/skale-me/skale-engine/pull/71) ([mvertes](https://github.com/mvertes))
- spillToDisk: do not duplicate memory when writing to disk [\#70](https://github.com/skale-me/skale-engine/pull/70) ([mvertes](https://github.com/mvertes))

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