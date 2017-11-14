# Skale-engine Roadmap

*Last updated December 29 2016*

This document describes the high level features the Skale-engine
maintainers have decided to prioritize in the near to medium term.

## Schema description for datasets

This would be useful for several purposes:

- support of columnar, such as parquet
- integration with a query language compatible with B.I. tools
- optimization of datasets serialization and transfers

## Add support for Parquet

*Status: in progress*

Parquet is a columnar storage format from the Apache Software
Foundation available to any project in the Hadoop ecosystem.

A separate nodeJS module supporting Apache parquet format, both for
reading an writing is first required.

Such an experimental module has been started by skale
maintainers [here](https://github.com/mvertes/node-parquet)

## Add support for Avro

Avro is a data serialization system from the Apache Software
Foundation which provides rich data structures and a compact fast
binary data format.

It is well suited to structured data where a schema is required to
encode and decode values.

A pure Javascript implementation exists
[here](https://github.com/mtth/avsc).

## Add realtime streaming capabilities

The current processing model is *action* driven, suitable for batch,
or micro-batch processing. See if it is possible to apply the same
API, or at least a subset, to *source* driven processing better
suited for realtime data processing, while retaining skale
scalability and efficiency.
