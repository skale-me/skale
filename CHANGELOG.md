# Change Log

## [1.2.0](https://github.com/skale-me/skale/tree/1.2.0) (2017-11-14)
[Full Changelog](https://github.com/skale-me/skale/compare/0.8.0...1.2.0)

**Closed issues:**

- Import skale-ml, skale-examples, skale-cli [\#181](https://github.com/skale-me/skale/issues/181)

## [0.8.0](https://github.com/skale-me/skale/tree/0.8.0) (2017-11-14)
[Full Changelog](https://github.com/skale-me/skale/compare/0.7.2...0.8.0)

**Closed issues:**

- Add missing tests, refactor testing framework [\#205](https://github.com/skale-me/skale/issues/205)
- External modules in worker [\#203](https://github.com/skale-me/skale/issues/203)
- for loop over items used later as index [\#178](https://github.com/skale-me/skale/issues/178)

**Merged pull requests:**

- rename skale-engine to skale \(\#181\) [\#230](https://github.com/skale-me/skale/pull/230) ([mvertes](https://github.com/mvertes))
- remove deprecated examples/ml/benchmark [\#229](https://github.com/skale-me/skale/pull/229) ([mvertes](https://github.com/mvertes))
- remove deprecated ml/examples/logreg [\#228](https://github.com/skale-me/skale/pull/228) ([mvertes](https://github.com/mvertes))
- ml: document StandardScaler\(\) [\#227](https://github.com/skale-me/skale/pull/227) ([mvertes](https://github.com/mvertes))
- ml: document classificationMetrics [\#226](https://github.com/skale-me/skale/pull/226) ([mvertes](https://github.com/mvertes))
- Document sgd.predict\(\) [\#225](https://github.com/skale-me/skale/pull/225) ([mvertes](https://github.com/mvertes))
- document sc.require\(\). Fix examples [\#224](https://github.com/skale-me/skale/pull/224) ([mvertes](https://github.com/mvertes))
- improve docs [\#223](https://github.com/skale-me/skale/pull/223) ([mvertes](https://github.com/mvertes))
- improve docs [\#222](https://github.com/skale-me/skale/pull/222) ([mvertes](https://github.com/mvertes))
- fix link [\#221](https://github.com/skale-me/skale/pull/221) ([mvertes](https://github.com/mvertes))
- fix eslint warnings [\#220](https://github.com/skale-me/skale/pull/220) ([mvertes](https://github.com/mvertes))
- update doc site [\#219](https://github.com/skale-me/skale/pull/219) ([mvertes](https://github.com/mvertes))
- Convert DataSet Object and related functions to modern JS [\#218](https://github.com/skale-me/skale/pull/218) ([frytyler](https://github.com/frytyler))
- avoid using require.resolve\(\) which may fail for old node versions [\#217](https://github.com/skale-me/skale/pull/217) ([mvertes](https://github.com/mvertes))
- Rename tape-test in test, get rid of mocha, resolves \#205 [\#216](https://github.com/skale-me/skale/pull/216) ([mvertes](https://github.com/mvertes))
- add new tests as per \#205 [\#215](https://github.com/skale-me/skale/pull/215) ([mvertes](https://github.com/mvertes))
- additional tests, as per \#205 [\#214](https://github.com/skale-me/skale/pull/214) ([mvertes](https://github.com/mvertes))
- additional tests, as per \#205 [\#213](https://github.com/skale-me/skale/pull/213) ([mvertes](https://github.com/mvertes))
- tests: migrate to tape, as per \#205 [\#212](https://github.com/skale-me/skale/pull/212) ([mvertes](https://github.com/mvertes))
- Implement dependency injection into workers, resolves \#203 [\#211](https://github.com/skale-me/skale/pull/211) ([mvertes](https://github.com/mvertes))
- docs: modularize, add logo [\#208](https://github.com/skale-me/skale/pull/208) ([mvertes](https://github.com/mvertes))
- Pass context to worker callbacks, add modules setting as per \#203 \(in progress\) [\#207](https://github.com/skale-me/skale/pull/207) ([mvertes](https://github.com/mvertes))
- test: start to add tape test cases, as per \#205 [\#206](https://github.com/skale-me/skale/pull/206) ([mvertes](https://github.com/mvertes))
- tests: simplify engine. Use standalone mode [\#204](https://github.com/skale-me/skale/pull/204) ([mvertes](https://github.com/mvertes))
- update dependencies [\#202](https://github.com/skale-me/skale/pull/202) ([mvertes](https://github.com/mvertes))
- lint: do not scan site/ [\#201](https://github.com/skale-me/skale/pull/201) ([mvertes](https://github.com/mvertes))
- doc: fix deploy rule [\#200](https://github.com/skale-me/skale/pull/200) ([mvertes](https://github.com/mvertes))
- doc: use mkdocs-material for documentation website. Add build recipes. [\#198](https://github.com/skale-me/skale/pull/198) ([mvertes](https://github.com/mvertes))
- doc: rebuild toc [\#197](https://github.com/skale-me/skale/pull/197) ([mvertes](https://github.com/mvertes))
- fix ml.KMeans, add example based on classical iris dataset [\#196](https://github.com/skale-me/skale/pull/196) ([mvertes](https://github.com/mvertes))
- fix sample\(\). Add takeSample\(\). [\#195](https://github.com/skale-me/skale/pull/195) ([mvertes](https://github.com/mvertes))
- now compute area under Precision Recall Curve, improve examples [\#194](https://github.com/skale-me/skale/pull/194) ([mvertes](https://github.com/mvertes))
- rename binary-classification-metrics to classification-metrics [\#193](https://github.com/skale-me/skale/pull/193) ([mvertes](https://github.com/mvertes))
- README: add link to code of conduct and API reference [\#192](https://github.com/skale-me/skale/pull/192) ([mvertes](https://github.com/mvertes))
- Create CODE\_OF\_CONDUCT.md [\#191](https://github.com/skale-me/skale/pull/191) ([mvertes](https://github.com/mvertes))
- ml: partition adult dataset, add headers in CSV files [\#190](https://github.com/skale-me/skale/pull/190) ([mvertes](https://github.com/mvertes))
- ml: rename SGDClassifier in SGDLinearModel, add regression example [\#189](https://github.com/skale-me/skale/pull/189) ([mvertes](https://github.com/mvertes))
- ml: remove deprecated logistic-regression dependency [\#188](https://github.com/skale-me/skale/pull/188) ([mvertes](https://github.com/mvertes))
- ml SGDClassifier: include optional intercept fitting [\#187](https://github.com/skale-me/skale/pull/187) ([mvertes](https://github.com/mvertes))
- ml: add SGDClassifier class [\#186](https://github.com/skale-me/skale/pull/186) ([mvertes](https://github.com/mvertes))
- contributing: add notes about documentation and coding rules [\#185](https://github.com/skale-me/skale/pull/185) ([mvertes](https://github.com/mvertes))
- ml: simplify a lot model evaluation metrics. Compute ROC AUC. [\#184](https://github.com/skale-me/skale/pull/184) ([mvertes](https://github.com/mvertes))
- Fix import of ml [\#183](https://github.com/skale-me/skale/pull/183) ([mvertes](https://github.com/mvertes))
- import skale-ml, as per \#181 [\#182](https://github.com/skale-me/skale/pull/182) ([mvertes](https://github.com/mvertes))
- update dependencies [\#180](https://github.com/skale-me/skale/pull/180) ([mvertes](https://github.com/mvertes))
- fixed the use of '===' operator in all sources [\#177](https://github.com/skale-me/skale/pull/177) ([vsimko](https://github.com/vsimko))
- travis: add osx target [\#176](https://github.com/skale-me/skale/pull/176) ([mvertes](https://github.com/mvertes))
- Fix debug trace, update dependencies [\#175](https://github.com/skale-me/skale/pull/175) ([mvertes](https://github.com/mvertes))
- aggregateByKey: fix a bug where undefined key crashes post-shuffle [\#173](https://github.com/skale-me/skale/pull/173) ([mvertes](https://github.com/mvertes))
- save: add support to CSV format output [\#172](https://github.com/skale-me/skale/pull/172) ([mvertes](https://github.com/mvertes))
- Automatically forward AWS env variables in context and workers [\#171](https://github.com/skale-me/skale/pull/171) ([mvertes](https://github.com/mvertes))
- workers: control garbage collect by command line option. Improve debug traces [\#170](https://github.com/skale-me/skale/pull/170) ([mvertes](https://github.com/mvertes))

## [0.7.2](https://github.com/skale-me/skale/tree/0.7.2) (2017-06-27)
[Full Changelog](https://github.com/skale-me/skale/compare/0.7.1...0.7.2)

**Merged pull requests:**

- update dependencies build files [\#169](https://github.com/skale-me/skale/pull/169) ([mvertes](https://github.com/mvertes))
- update dependencies [\#168](https://github.com/skale-me/skale/pull/168) ([mvertes](https://github.com/mvertes))
- workers: reduce amount of traces during shuffle [\#167](https://github.com/skale-me/skale/pull/167) ([mvertes](https://github.com/mvertes))
- worker: manually call garbage collector at end of task. Experimental [\#166](https://github.com/skale-me/skale/pull/166) ([mvertes](https://github.com/mvertes))
- add interactive REPL shell, supporting async/await [\#165](https://github.com/skale-me/skale/pull/165) ([mvertes](https://github.com/mvertes))
- Fix and improved tracing of time measurements [\#164](https://github.com/skale-me/skale/pull/164) ([mvertes](https://github.com/mvertes))
- worker: allow to retry connections using -r \<nbretry\> option [\#163](https://github.com/skale-me/skale/pull/163) ([mvertes](https://github.com/mvertes))
- dataset: fix parsing of shuffle in aggregateByKey, clean code [\#162](https://github.com/skale-me/skale/pull/162) ([mvertes](https://github.com/mvertes))
- performance: block processing for pipelines at source level, part 2 [\#161](https://github.com/skale-me/skale/pull/161) ([mvertes](https://github.com/mvertes))
- eslint: allow arrow functions and async/await [\#160](https://github.com/skale-me/skale/pull/160) ([mvertes](https://github.com/mvertes))
- Fix previous task serialization change which impacted sc.range\(\) [\#159](https://github.com/skale-me/skale/pull/159) ([mvertes](https://github.com/mvertes))
- Clean code, improve traces for performances, set maximum partitions. [\#158](https://github.com/skale-me/skale/pull/158) ([mvertes](https://github.com/mvertes))
- update dependencies [\#157](https://github.com/skale-me/skale/pull/157) ([mvertes](https://github.com/mvertes))
- textFile: Fix handling of gzipped files in local filesystem [\#156](https://github.com/skale-me/skale/pull/156) ([mvertes](https://github.com/mvertes))

## [0.7.1](https://github.com/skale-me/skale/tree/0.7.1) (2017-05-17)
[Full Changelog](https://github.com/skale-me/skale/compare/0.7.0...0.7.1)

**Merged pull requests:**

- API doc: add a section on environment variables [\#155](https://github.com/skale-me/skale/pull/155) ([mvertes](https://github.com/mvertes))
- doc: fix some typos [\#154](https://github.com/skale-me/skale/pull/154) ([mvertes](https://github.com/mvertes))
- Fix a worker crash when using ds.sample\(\) with replacement. [\#153](https://github.com/skale-me/skale/pull/153) ([mvertes](https://github.com/mvertes))
- add sample docker files [\#152](https://github.com/skale-me/skale/pull/152) ([mvertes](https://github.com/mvertes))
- doc: fix links [\#151](https://github.com/skale-me/skale/pull/151) ([mvertes](https://github.com/mvertes))
- mention the Skale Hacker's Guide [\#150](https://github.com/skale-me/skale/pull/150) ([mvertes](https://github.com/mvertes))
- add a Skale Hacker's Guide [\#149](https://github.com/skale-me/skale/pull/149) ([mvertes](https://github.com/mvertes))
- Clean up dependency, more info at worker-controller connection [\#147](https://github.com/skale-me/skale/pull/147) ([mvertes](https://github.com/mvertes))
- Added .npmignore [\#146](https://github.com/skale-me/skale/pull/146) ([mvertes](https://github.com/mvertes))
- worker: retry initial network connection to server [\#145](https://github.com/skale-me/skale/pull/145) ([mvertes](https://github.com/mvertes))

## [0.7.0](https://github.com/skale-me/skale/tree/0.7.0) (2017-04-04)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.11...0.7.0)

**Fixed bugs:**

- textFile: fix a bug where the 1st file in S3 dir was skipped [\#126](https://github.com/skale-me/skale/pull/126) ([mvertes](https://github.com/mvertes))

**Closed issues:**

- skale-engine version 0.5.3 regression? [\#61](https://github.com/skale-me/skale/issues/61)
- sizeOf is incomplete and inaccurate [\#52](https://github.com/skale-me/skale/issues/52)

**Merged pull requests:**

- update dependencies [\#143](https://github.com/skale-me/skale/pull/143) ([mvertes](https://github.com/mvertes))
- textFile, save: document new protocols and formats [\#142](https://github.com/skale-me/skale/pull/142) ([mvertes](https://github.com/mvertes))
- save: support direct streaming to S3 [\#141](https://github.com/skale-me/skale/pull/141) ([mvertes](https://github.com/mvertes))
- textFile: support file globbing \(i.e. src/\*\*/\*.js\) in path argument [\#140](https://github.com/skale-me/skale/pull/140) ([mvertes](https://github.com/mvertes))
- save: support direct streaming to azure. [\#139](https://github.com/skale-me/skale/pull/139) ([mvertes](https://github.com/mvertes))
- azure: handle server errors with internal retry policy filter [\#138](https://github.com/skale-me/skale/pull/138) ([mvertes](https://github.com/mvertes))
- AggregateByKey: faster processing of shuffle files [\#137](https://github.com/skale-me/skale/pull/137) ([mvertes](https://github.com/mvertes))
- aggregateByKey, coGroup: performance improvements [\#136](https://github.com/skale-me/skale/pull/136) ([mvertes](https://github.com/mvertes))
- Performance increase of aggregateByKey and coGroup [\#135](https://github.com/skale-me/skale/pull/135) ([mvertes](https://github.com/mvertes))
- azure: handle errors from storage server: fetch before process and implement retries. [\#134](https://github.com/skale-me/skale/pull/134) ([mvertes](https://github.com/mvertes))
- distributed mode: pre-fork new workers at end of master [\#133](https://github.com/skale-me/skale/pull/133) ([mvertes](https://github.com/mvertes))
- textFile: fix compute of partitions for Azure [\#132](https://github.com/skale-me/skale/pull/132) ([mvertes](https://github.com/mvertes))
- save: upload datasets to azure for urls in wasb:// [\#131](https://github.com/skale-me/skale/pull/131) ([mvertes](https://github.com/mvertes))
- textFile: support file globbing, azure blobs only for now [\#130](https://github.com/skale-me/skale/pull/130) ([mvertes](https://github.com/mvertes))
- textFile: support reading from azure storage blobs [\#129](https://github.com/skale-me/skale/pull/129) ([mvertes](https://github.com/mvertes))
- Simplify top and take actions, and fix out of memory errors for large datasets. [\#128](https://github.com/skale-me/skale/pull/128) ([mvertes](https://github.com/mvertes))
- Aggregate now guarantees partitions ordering. Improve traces. [\#127](https://github.com/skale-me/skale/pull/127) ([mvertes](https://github.com/mvertes))
- textFile: read parquet from local and S3 directories [\#125](https://github.com/skale-me/skale/pull/125) ([mvertes](https://github.com/mvertes))
- Add Parquet support [\#124](https://github.com/skale-me/skale/pull/124) ([mvertes](https://github.com/mvertes))
- Readme: Fix appveyor badge [\#123](https://github.com/skale-me/skale/pull/123) ([mvertes](https://github.com/mvertes))
- fix benchmark [\#122](https://github.com/skale-me/skale/pull/122) ([mvertes](https://github.com/mvertes))
- Fix handling internal dependency on self. [\#121](https://github.com/skale-me/skale/pull/121) ([mvertes](https://github.com/mvertes))

## [0.6.11](https://github.com/skale-me/skale/tree/0.6.11) (2017-02-09)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.10...0.6.11)

**Merged pull requests:**

- fix regression at install [\#120](https://github.com/skale-me/skale/pull/120) ([mvertes](https://github.com/mvertes))

## [0.6.10](https://github.com/skale-me/skale/tree/0.6.10) (2017-02-09)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.9...0.6.10)

**Merged pull requests:**

- Add node 6 target for travis, use system default memory settings [\#119](https://github.com/skale-me/skale/pull/119) ([mvertes](https://github.com/mvertes))
- Allow to run on windows [\#118](https://github.com/skale-me/skale/pull/118) ([mvertes](https://github.com/mvertes))

## [0.6.9](https://github.com/skale-me/skale/tree/0.6.9) (2017-01-02)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.8...0.6.9)

**Merged pull requests:**

- textFile: add a maxFiles option, to limit the number of files to process [\#117](https://github.com/skale-me/skale/pull/117) ([mvertes](https://github.com/mvertes))
- Increase number of streams over skale protocol [\#116](https://github.com/skale-me/skale/pull/116) ([mvertes](https://github.com/mvertes))
- stream\(\) action: add option to terminate context on stream end [\#115](https://github.com/skale-me/skale/pull/115) ([mvertes](https://github.com/mvertes))
- doc: Fix TOC link [\#114](https://github.com/skale-me/skale/pull/114) ([mvertes](https://github.com/mvertes))

## [0.6.8](https://github.com/skale-me/skale/tree/0.6.8) (2016-12-14)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.7...0.6.8)

**Fixed bugs:**

- In distributed mode, temporary files are not deleted at end of program [\#110](https://github.com/skale-me/skale/issues/110)

**Merged pull requests:**

- Advertise benchmark in README [\#113](https://github.com/skale-me/skale/pull/113) ([mvertes](https://github.com/mvertes))
- server, worker-controller: add current statistics [\#112](https://github.com/skale-me/skale/pull/112) ([mvertes](https://github.com/mvertes))
- Fix bug \#110 where temporary files were not deleted at end of task in… [\#111](https://github.com/skale-me/skale/pull/111) ([mvertes](https://github.com/mvertes))
- use SKALE\_WORKERS to set number of workers in distributed mode, as in… [\#109](https://github.com/skale-me/skale/pull/109) ([mvertes](https://github.com/mvertes))
- update dependencies [\#108](https://github.com/skale-me/skale/pull/108) ([mvertes](https://github.com/mvertes))
- Doc: add a section on core concepts, label shuffle transforms [\#107](https://github.com/skale-me/skale/pull/107) ([mvertes](https://github.com/mvertes))
- Refactor internal Task API, to propagate env and dependencies. In pro… [\#106](https://github.com/skale-me/skale/pull/106) ([mvertes](https://github.com/mvertes))

## [0.6.7](https://github.com/skale-me/skale/tree/0.6.7) (2016-11-22)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.6...0.6.7)

**Merged pull requests:**

- Document standalone and distributed modes [\#105](https://github.com/skale-me/skale/pull/105) ([mvertes](https://github.com/mvertes))
- Distributed mode: Implement peer-to-peer through HTTP for shuffle data transfer [\#104](https://github.com/skale-me/skale/pull/104) ([mvertes](https://github.com/mvertes))
- Improve traces and file transfer. [\#103](https://github.com/skale-me/skale/pull/103) ([mvertes](https://github.com/mvertes))
- Improve task scheduling [\#102](https://github.com/skale-me/skale/pull/102) ([mvertes](https://github.com/mvertes))
- Fix mocha dependency. Skip yarn files. [\#101](https://github.com/skale-me/skale/pull/101) ([mvertes](https://github.com/mvertes))

## [0.6.6](https://github.com/skale-me/skale/tree/0.6.6) (2016-11-04)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.5...0.6.6)

**Merged pull requests:**

- Task serialization is now correct in all cases. [\#100](https://github.com/skale-me/skale/pull/100) ([mvertes](https://github.com/mvertes))
- Optimize task data transfer by sending sparse datasets. [\#99](https://github.com/skale-me/skale/pull/99) ([mvertes](https://github.com/mvertes))
- Preliminary work for optimization of task data transfer [\#98](https://github.com/skale-me/skale/pull/98) ([mvertes](https://github.com/mvertes))
- improve getReadStream [\#97](https://github.com/skale-me/skale/pull/97) ([mvertes](https://github.com/mvertes))
- distributed mode: fixes in compressed transfers [\#96](https://github.com/skale-me/skale/pull/96) ([mvertes](https://github.com/mvertes))
- Improve distributed mode [\#95](https://github.com/skale-me/skale/pull/95) ([mvertes](https://github.com/mvertes))
- distributed mode: protocol debug traces enabled with SKALE\_DEBUG=3 [\#94](https://github.com/skale-me/skale/pull/94) ([mvertes](https://github.com/mvertes))
- sc.textFile: fix handling of S3 directories [\#93](https://github.com/skale-me/skale/pull/93) ([mvertes](https://github.com/mvertes))
- Fix log in distributed worker [\#92](https://github.com/skale-me/skale/pull/92) ([mvertes](https://github.com/mvertes))

## [0.6.5](https://github.com/skale-me/skale/tree/0.6.5) (2016-10-23)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.4...0.6.5)

**Merged pull requests:**

- bin/worker.js: fix typo in dependencies [\#91](https://github.com/skale-me/skale/pull/91) ([mvertes](https://github.com/mvertes))
- Debug traces sent to stderr instead of stdout [\#90](https://github.com/skale-me/skale/pull/90) ([mvertes](https://github.com/mvertes))
- save to S3: extend http timeout to 1h [\#89](https://github.com/skale-me/skale/pull/89) ([mvertes](https://github.com/mvertes))

## [0.6.4](https://github.com/skale-me/skale/tree/0.6.4) (2016-10-11)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.3...0.6.4)

**Merged pull requests:**

- dataset stream and save: preserve structure [\#88](https://github.com/skale-me/skale/pull/88) ([mvertes](https://github.com/mvertes))
- textFile: add capability to handle single AWS S3 files, gzipped or not. [\#87](https://github.com/skale-me/skale/pull/87) ([mvertes](https://github.com/mvertes))

## [0.6.3](https://github.com/skale-me/skale/tree/0.6.3) (2016-10-08)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.2...0.6.3)

**Merged pull requests:**

- Add new stream action, which allows to stream out a dataset [\#86](https://github.com/skale-me/skale/pull/86) ([mvertes](https://github.com/mvertes))

## [0.6.2](https://github.com/skale-me/skale/tree/0.6.2) (2016-09-14)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.1...0.6.2)

**Closed issues:**

- bin/server doesn't use nworker parameter [\#82](https://github.com/skale-me/skale/issues/82)

**Merged pull requests:**

- Fix save\(\) action to export a dataset to filesystem or S3 [\#84](https://github.com/skale-me/skale/pull/84) ([mvertes](https://github.com/mvertes))
- Remove unused nworker command line parameter [\#83](https://github.com/skale-me/skale/pull/83) ([mark-bradshaw](https://github.com/mark-bradshaw))
- Minor grammar update. [\#80](https://github.com/skale-me/skale/pull/80) ([mark-bradshaw](https://github.com/mark-bradshaw))
- Add streaming source from AWS S3 [\#78](https://github.com/skale-me/skale/pull/78) ([mvertes](https://github.com/mvertes))
- local worker: handle master disconnect [\#77](https://github.com/skale-me/skale/pull/77) ([mvertes](https://github.com/mvertes))
- Increase buffer size of zlib from 16kB to 64kB. Better performances [\#76](https://github.com/skale-me/skale/pull/76) ([mvertes](https://github.com/mvertes))
- textFile now supports directory as dataset source [\#75](https://github.com/skale-me/skale/pull/75) ([mvertes](https://github.com/mvertes))
- Coding style change. We now use the same as NodeJS core. [\#74](https://github.com/skale-me/skale/pull/74) ([mvertes](https://github.com/mvertes))

## [0.6.1](https://github.com/skale-me/skale/tree/0.6.1) (2016-07-05)
[Full Changelog](https://github.com/skale-me/skale/compare/0.6.0...0.6.1)

**Merged pull requests:**

- Important improvements in task serialization, scheduling and debugging [\#73](https://github.com/skale-me/skale/pull/73) ([mvertes](https://github.com/mvertes))
- save: reset file at init [\#72](https://github.com/skale-me/skale/pull/72) ([mvertes](https://github.com/mvertes))
- Add SKALE\_MEMORY env variable to set worker max memory [\#71](https://github.com/skale-me/skale/pull/71) ([mvertes](https://github.com/mvertes))
- spillToDisk: do not duplicate memory when writing to disk [\#70](https://github.com/skale-me/skale/pull/70) ([mvertes](https://github.com/mvertes))

## [0.6.0](https://github.com/skale-me/skale/tree/0.6.0) (2016-06-24)
[Full Changelog](https://github.com/skale-me/skale/compare/0.5.3...0.6.0)

**Merged pull requests:**

- stop testing node-0.10 [\#69](https://github.com/skale-me/skale/pull/69) ([mvertes](https://github.com/mvertes))
- add new source gzipFile\(\) to process gzipped text files [\#68](https://github.com/skale-me/skale/pull/68) ([mvertes](https://github.com/mvertes))
- New action Dataset\#save\(\) to save dataset content in text file. [\#67](https://github.com/skale-me/skale/pull/67) ([mvertes](https://github.com/mvertes))
- Pass user options and worker context to reduce\(\), aggregate\(\) and forEach\(\) callbacks. [\#66](https://github.com/skale-me/skale/pull/66) ([mvertes](https://github.com/mvertes))
- Serverless version, direct IPCs between workers and master [\#65](https://github.com/skale-me/skale/pull/65) ([mvertes](https://github.com/mvertes))
- code cleaning [\#64](https://github.com/skale-me/skale/pull/64) ([mvertes](https://github.com/mvertes))
- improve tmp dir handling. Add env SKALE\_TMP [\#63](https://github.com/skale-me/skale/pull/63) ([mvertes](https://github.com/mvertes))
- remove spurious test file [\#62](https://github.com/skale-me/skale/pull/62) ([mvertes](https://github.com/mvertes))

## [0.5.3](https://github.com/skale-me/skale/tree/0.5.3) (2016-05-17)
[Full Changelog](https://github.com/skale-me/skale/compare/0.5.2...0.5.3)

**Closed issues:**

- skale-engine can not be used as a dependency in a Meteor project [\#56](https://github.com/skale-me/skale/issues/56)
- Get rid of direct eval [\#53](https://github.com/skale-me/skale/issues/53)
- Use mkdirp.sync instead of lib/mkdir.js [\#51](https://github.com/skale-me/skale/issues/51)

**Merged pull requests:**

- rename lib/sizeof.js in lib/rough-sizeof.js as discussed in \#52 [\#60](https://github.com/skale-me/skale/pull/60) ([mvertes](https://github.com/mvertes))
- Use indirect eval, strenghten regexp which match arrow function definition; fix \#53 [\#59](https://github.com/skale-me/skale/pull/59) ([mvertes](https://github.com/mvertes))
- remove lib/mkdir.js and use external mkdirp module. Fix \#51 [\#58](https://github.com/skale-me/skale/pull/58) ([mvertes](https://github.com/mvertes))
- Ensure compatibility with node back to 0.10, fix \#56 [\#57](https://github.com/skale-me/skale/pull/57) ([mvertes](https://github.com/mvertes))
- better use of console.log\(\) [\#50](https://github.com/skale-me/skale/pull/50) ([mvertes](https://github.com/mvertes))
- ds.filter\(\): rename internal filter member into \_filter to avoid coll… [\#49](https://github.com/skale-me/skale/pull/49) ([mvertes](https://github.com/mvertes))

## [0.5.2](https://github.com/skale-me/skale/tree/0.5.2) (2016-05-04)
[Full Changelog](https://github.com/skale-me/skale/compare/0.5.1...0.5.2)

**Merged pull requests:**

- Fix previous changes incompatible with node4-LTS [\#48](https://github.com/skale-me/skale/pull/48) ([mvertes](https://github.com/mvertes))

## [0.5.1](https://github.com/skale-me/skale/tree/0.5.1) (2016-05-04)
[Full Changelog](https://github.com/skale-me/skale/compare/0.5.0...0.5.1)

**Merged pull requests:**

- doc: fix doc on actions [\#47](https://github.com/skale-me/skale/pull/47) ([mvertes](https://github.com/mvertes))
- fix ds.first\(\) to return an element instead of an array [\#46](https://github.com/skale-me/skale/pull/46) ([mvertes](https://github.com/mvertes))
- remove lib/ml.js, restructure examples [\#45](https://github.com/skale-me/skale/pull/45) ([mvertes](https://github.com/mvertes))
- various fixes [\#44](https://github.com/skale-me/skale/pull/44) ([mvertes](https://github.com/mvertes))

## [0.5.0](https://github.com/skale-me/skale/tree/0.5.0) (2016-05-02)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.5...0.5.0)

**Merged pull requests:**

- benchmark: update to new action syntax [\#43](https://github.com/skale-me/skale/pull/43) ([mvertes](https://github.com/mvertes))
- Remove toArray\(\). Fix examples [\#42](https://github.com/skale-me/skale/pull/42) ([mvertes](https://github.com/mvertes))
- All actions take an optional callback and return promises instead of … [\#41](https://github.com/skale-me/skale/pull/41) ([mvertes](https://github.com/mvertes))
- simplify JS benchmark code to make it almost identical to python version [\#40](https://github.com/skale-me/skale/pull/40) ([mvertes](https://github.com/mvertes))
- add benchmark [\#38](https://github.com/skale-me/skale/pull/38) ([mvertes](https://github.com/mvertes))

## [0.4.5](https://github.com/skale-me/skale/tree/0.4.5) (2016-04-25)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.4...0.4.5)

**Merged pull requests:**

- Provide a faster sizeof, without external dependency [\#37](https://github.com/skale-me/skale/pull/37) ([mvertes](https://github.com/mvertes))
- code cleaning [\#36](https://github.com/skale-me/skale/pull/36) ([mvertes](https://github.com/mvertes))
- fix broken arrow function example [\#35](https://github.com/skale-me/skale/pull/35) ([mvertes](https://github.com/mvertes))
- Command line option to set max  memory per worker [\#34](https://github.com/skale-me/skale/pull/34) ([mvertes](https://github.com/mvertes))
- master: set process title to ease monitoring [\#33](https://github.com/skale-me/skale/pull/33) ([mvertes](https://github.com/mvertes))

## [0.4.4](https://github.com/skale-me/skale/tree/0.4.4) (2016-04-20)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.3...0.4.4)

**Merged pull requests:**

- worker: fix init of Partition.mm [\#32](https://github.com/skale-me/skale/pull/32) ([mvertes](https://github.com/mvertes))

## [0.4.3](https://github.com/skale-me/skale/tree/0.4.3) (2016-04-20)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.2...0.4.3)

**Merged pull requests:**

- doc: various fixes [\#31](https://github.com/skale-me/skale/pull/31) ([mvertes](https://github.com/mvertes))
- Dataset: provide a better RNG. Expose Random and Poisson classes. [\#30](https://github.com/skale-me/skale/pull/30) ([mvertes](https://github.com/mvertes))
- On workers: evict a partition if going out of memory [\#29](https://github.com/skale-me/skale/pull/29) ([mvertes](https://github.com/mvertes))
- Handle connection error to terminate a master if a worker fails [\#28](https://github.com/skale-me/skale/pull/28) ([mvertes](https://github.com/mvertes))

## [0.4.2](https://github.com/skale-me/skale/tree/0.4.2) (2016-04-17)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.1...0.4.2)

**Merged pull requests:**

- Preliminary work to isolate machine learning capabilities from core engine [\#27](https://github.com/skale-me/skale/pull/27) ([CedricArtigue](https://github.com/CedricArtigue))
- fix lint [\#26](https://github.com/skale-me/skale/pull/26) ([mvertes](https://github.com/mvertes))
- run eslint in pre-test [\#25](https://github.com/skale-me/skale/pull/25) ([mvertes](https://github.com/mvertes))
- fix lint problems. Remove dead code [\#24](https://github.com/skale-me/skale/pull/24) ([mvertes](https://github.com/mvertes))
- new lint fixes [\#23](https://github.com/skale-me/skale/pull/23) ([mvertes](https://github.com/mvertes))
- Simplify code, fix lint errors. More to come. [\#22](https://github.com/skale-me/skale/pull/22) ([mvertes](https://github.com/mvertes))
- add eslint. Start code cleaning [\#21](https://github.com/skale-me/skale/pull/21) ([mvertes](https://github.com/mvertes))

## [0.4.1](https://github.com/skale-me/skale/tree/0.4.1) (2016-04-07)
[Full Changelog](https://github.com/skale-me/skale/compare/0.4.0...0.4.1)

**Merged pull requests:**

- document partitioners [\#20](https://github.com/skale-me/skale/pull/20) ([mvertes](https://github.com/mvertes))
- doc: fix cross references [\#19](https://github.com/skale-me/skale/pull/19) ([mvertes](https://github.com/mvertes))
- document ds.top\(\) [\#18](https://github.com/skale-me/skale/pull/18) ([mvertes](https://github.com/mvertes))
- document ds.take\(\) [\#17](https://github.com/skale-me/skale/pull/17) ([mvertes](https://github.com/mvertes))
- document ds.sortByKey\(\) [\#16](https://github.com/skale-me/skale/pull/16) ([mvertes](https://github.com/mvertes))
- document ds.sortBy\(\) [\#15](https://github.com/skale-me/skale/pull/15) ([mvertes](https://github.com/mvertes))
- document ds.persist\(\) [\#14](https://github.com/skale-me/skale/pull/14) ([mvertes](https://github.com/mvertes))
- Document ds.partitionBy\(\) and fix example [\#13](https://github.com/skale-me/skale/pull/13) ([mvertes](https://github.com/mvertes))
- document ds.first\(\) [\#12](https://github.com/skale-me/skale/pull/12) ([mvertes](https://github.com/mvertes))
- document ds.aggregateByKey\(\) [\#11](https://github.com/skale-me/skale/pull/11) ([mvertes](https://github.com/mvertes))
- aggregateByKey: change args order to match aggregate [\#10](https://github.com/skale-me/skale/pull/10) ([mvertes](https://github.com/mvertes))
- document sc.range\(\) [\#9](https://github.com/skale-me/skale/pull/9) ([mvertes](https://github.com/mvertes))
- use sizeof from external npm dependency [\#8](https://github.com/skale-me/skale/pull/8) ([mvertes](https://github.com/mvertes))
- rename RDD in dataset. No functional change. [\#7](https://github.com/skale-me/skale/pull/7) ([mvertes](https://github.com/mvertes))

## [0.4.0](https://github.com/skale-me/skale/tree/0.4.0) (2016-04-04)
[Full Changelog](https://github.com/skale-me/skale/compare/0.3.5...0.4.0)

**Merged pull requests:**

- All actions now return a readable stream [\#6](https://github.com/skale-me/skale/pull/6) ([mvertes](https://github.com/mvertes))

## [0.3.5](https://github.com/skale-me/skale/tree/0.3.5) (2016-04-03)
[Full Changelog](https://github.com/skale-me/skale/compare/0.3.4...0.3.5)

**Fixed bugs:**

- cartesian incorrect output [\#4](https://github.com/skale-me/skale/issues/4)

**Merged pull requests:**

- lib/dataset: cartesian.iterate\(\): fix partition index computation. Fix \#4 [\#5](https://github.com/skale-me/skale/pull/5) ([mvertes](https://github.com/mvertes))
- .travis.yml: set a fixed number of workers \(4\)  [\#3](https://github.com/skale-me/skale/pull/3) ([mvertes](https://github.com/mvertes))

## [0.3.4](https://github.com/skale-me/skale/tree/0.3.4) (2016-04-01)
[Full Changelog](https://github.com/skale-me/skale/compare/0.3.3...0.3.4)

**Fixed bugs:**

- examples/core/parallelize.js fails with 2 workers [\#2](https://github.com/skale-me/skale/issues/2)

## [0.3.3](https://github.com/skale-me/skale/tree/0.3.3) (2016-03-25)
[Full Changelog](https://github.com/skale-me/skale/compare/0.3.2...0.3.3)

## [0.3.2](https://github.com/skale-me/skale/tree/0.3.2) (2016-03-23)
**Merged pull requests:**

- Add a Gitter chat badge to README.md [\#1](https://github.com/skale-me/skale/pull/1) ([gitter-badger](https://github.com/gitter-badger))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
