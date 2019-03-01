# WAL - write ahead log

This folder contains the code for a performant write ahead log.
It uses seastar as the underlying IO mechanism.

It offers a paradigm of `single-reader-single-writer`.
per `(namespace, topic, partition)`.

The WAL offers 2 types of topics.

> Important:
> Topic(s) and namespace(s) have a public api of xxhash64().
> That is, they are transfered in `text` only in the `create()`
> method call and from then on only referenced by their id.
> That is topic = xxhash_64(topic.c_str(), topic.size())
>         ns    = xxhash_64(namespace.c_str(), namespace.size())
>

### Regular Topics:

* Topics are broken down to `(namespace, topic, partition)` triplets.

* Operations can **only** happen on this datatype. (internally we 
  refer to this datatype as `nstpidx` for namespace, topic, partition
  index)

* A topic upon creation is an immutable object and will eventually be
  removed after data expiration *only*. There is no other mechanism.

* Users can attach properties to a `(namespace, topic, partition)` on creation.
  This is useful for RBAC/ABAC policies.

### Compaction Topics:

They are the same as Regular Topics with the addition of:

* It maintains a rolling `32MB` cache of keys in memory to do deduplication.
  see `wal_segment_indexer::kMaxPartialIndexKeysSize`
  
* It writes `an additional .index` file *per* `(namespace, topic, partition, segment)`
  tuple - where a `segment` is just a physical file of the log on disk.

* This `.index` file contains metadata for the background compaction thread and is typically
  small. If you see your index file to be very large, aproaching the size of the segment
  it means you are likely using compaction topics incorrectly.

* A typical usecase for a compaction topic would be a stock trading analytics. If you
  want to read the `latest` value of a particular stock, you can use compaction topics
  as the number of inserts into the WAL is high, but the dimensionality of the
  topic is smaller. MQTT has the `last testament` for example and many other such applications.

* Remember your resource usage of hardware when creating compaction topics.
  If your key space is `tiny` you might want to combine multiple compaction topics
  into one and do some lightweight filtering on the client side.

Assuming a namespace `my-namespace` and a topic `my-topic` with `16` partitions 
(also assuming `my-topic` is a `compaction` topic).
You might end up with a directory structure that looks like this:

```sh
ls -al /redpanda
```

would return something like this: 

```sh

/redpanda/my-namespace/my-topic/0/.
                                 /metadata
                                 /0.wal        -----> initial log segment
                                 /0.wal.index  -----> index for initial log segment
                                 ...
                                 /3986.wal
                                 /3986.wal.index
                       ...

                      /my-topic/16/.
                                 /metadata     -----> stores properties set by user at create 
                                 /0.wal
                                 /0.wal.index
                                 ...
                                 /3986.wal
                                 /3986.wal.index

```

> Implementation note. topics and namespaces can only contain ascii + `-` and `_`

## High Level Architecture

We use seastar as our building block. As such
all requests *must* be sharded. So any particular request
`will delegate` work to cores that `own` the data.

```

                           - core 0
 Request                    +---+
+-------+                  /- 3 |
| +---+ |                /- +---+
| | 1 | ---           /--
| +---+ |  \---     /-
| +---+ |      \-/--
| | 2 ---      /-  \---     - core 8
| +---+ |\--/--        \---  +---+
| +---+ | /- \----         \-- 1 |
| | 3 | -- -      \----      +---+
| +---+ |              \---  +---+
+-------+                  \-- 2 |
                             +---+
```

This mapping is abstracted in the `wal_core_mapping.h` file. 
Given any input, it will perform the hashing, and assignment
without copying any data, but rather referecing the data for reads.
This is important. We do not mutate the input data.

This also means that the receiving core does the dispatching 
to the other cores. See `chain_replication_service.cc` for an example.


## Per Shard Architecture
## Namespace/Topic/Partition
## Implementation Notes

* `wal_opts::max_bytes_in_writer_cache = 1MB`:
  All writers (`wal_writer_node.h`) will use `1MB` for in memory buffer at most.
  If the the writer is for a compaction topic, the memory usage is `2MB` +
  indexer cache. This is an upper bound. Once we flush pending writes, we free up memory.
  Example: 100 topics; each has 10 partitions.
  You will have `100 X 10 X 1MB = 16GB` of memory used
  just for flushing bytes to disk.
* `wal_opts::max_log_segment_size = 1GB`:
  We `fallocate()` our log segments to be 1GB by default. User can reconfigure.
  This has several performance benefits as it prevents the filesystem from having to
  acquire cross-core locks to update file-sizes and other metadata information per flush.
  From the user level, this has a `gotcha`. The user cannot issue a write bigger
  than one log segment.
  The second non-intuitive fact is that if a user does `ls -al /path/to/dir` the file
  sizes might be artificially high for the current open files.
  On failure, we recover file sizes that are `exactly 1GB or max_log_segment_size`, by
  reading all the `record-headers not values` and ensuring that there is data, truncating
  the log segment on the last record.
  if the record is not the last one, we print an error message and do not delete nor truncate
  the file.
* `wal_otps::max_retention_size = -1`:
  Users can opt-in to have both size-based and time-based policy for data expiration.
  The default is to only use time-based of 168hrs before deleting a log segment.
* `wal_opts::max_retention_period = 168hrs`:
  The default retention period for one log segment based on the last modified timestamp
  on the local filesystem is 168hrs. Users can have time-based eviction policy, or
  both time and size based. see `max_retention_size`
* `wal_opts::flush_period = 10seconds`:
  By default we flush the log segments every 10 seconds. This is a configurable time.
  A coordinated failure can mean data loss. If you loose a chain of machines `A-B-C`
  for a particular `(namespace, topic, partition)` within 10 seconds,
  you can loose data for *that* particular chain.
  The minimum allowed is `2milliseconds`. We enforce this to get some
  filesystem batching of data.


## Alternatives
## Unresolved Questions
* Integration w/ the [IOScheduler for compactions](
https://www.scylladb.com/2018/06/12/scylla-leverages-control-theory/)

* When all topic/partitions have expired we need to delete all the files and 
  the folders in those files (topic,partitions)
  Currently this is left up to the administrator and `rm -rf`
