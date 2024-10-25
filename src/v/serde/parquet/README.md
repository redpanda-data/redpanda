# Parquet Library

Here lies a library to be able to write parquet files for Redpanda's Iceberg integration.
Due to Redpanda's usage of Seastar off the shelf preexisting parquet libraries do not meet
our strict requirements, imposed by our userland task scheduler and virtual memory avoiding 
allocator.

### Record Shredding

Parquet shreds records into a columnar format using the [same algorithm][twitter-parquet] as published in the [Dremel
paper][dremel]. A very helpful step by step explainer of the algorithm can be found [here][random-useful-wiki].
Our implmentation of this can be found in `shredder.cc`.

[random-useful-wiki]: https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper#step-by-step
[twitter-parquet]: https://blog.x.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
[dremel]: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf

### Metadata

Parquet metadata is serialized using [Apache Thrift's compact wire format][thrift-compact-format].

We use metadata that is the logical representation of what our application needs, then we write out
the wire format with all the deprecated and legacy types to be compatible with legacy query systems.

The physical format of serialized parquet metadata is documented [here][parquet-thrift].


[parquet-thrift]: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
[thrift-compact-format]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
