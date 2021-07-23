# Schema Registry

## References

- REST API: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
- Alternative implementation: https://github.com/aiven/karapace

## High level design

Schema registry is built in to `pandaproxy`.

A single-partition topic `_schemas` is used as a backend store.

Each node maintains an in-memory copy of all schema data, lazily updated from the topic.

Active/Active HA: any node can serve reads and writes.

User-facing REST API is compatible with other products' schema registry implementations, but internal storage format is
not intended to be compatible.

*Future: we should apply a sanity limit on the number of schemas, as we hold them all in memory.*

## Writer synchronisation

**tldr; Optimistic writes, first writer wins**

Each write includes a sequence number in the key. This sequence number increases monotonically, and any write with an
out-of-order sequence number is ignored by readers. Writers set the sequence number for their next write as the most
recent message sequence they've seen, plus one.

While the sequence number is logically independent of the offset, it is very convenient to simply use the offset as the
sequence number:
that way, message validity can be checked by simply comparing the sequence number to the actual offset where the message
was written.

Under contention, writers know whether their write was successful by comparing the offset returned by their write
against the offset where they expected to write. If not equal, writer retries until they succeed.

Using the offset as the effective sequence number comes with a downside: concurrent writes from different nodes are more
expensive, as otherwise-valid writes can be rendered invalid by another /invalid/ write arriving before and knocking
them out of their intended offset. However, schemas are likely to be written at comparatively low rates, and usually by
a single API client (e.g. a CI system deploying new schemas for a new software build). This can be improved in future by
switching to a true sequence number (such that for a given sequence number under contended writes, at least one writer
is guaranteed to win).

Allocation of schema IDs and version IDs is done locally by writers based on the contents of their `ShardedStore`. If
this turns out to be out of date, then their write will retry (allocation is re-done on retry). Changes to the
`ShardedStore`, including advancing the next ID to allocate, are only applied once the write persists successfully.

To avoid compaction incorrectly preserving the last contended write (contended meaning same seq number) rather than
first write to a particular subject-version, the writer's node ID is also included in the key.

For permanent deletion of subjects or versions (i.e. writing tombstones), writers must remember the sequence number and
node ID that originally wrote to the subject, in order to regenerate the original write's key. These permanent deletion
writes are not subject to any sequence-ordering logic, and if multiple nodes perma-delete the same thing concurrently,
they will both succeed.

Q: Why is the sequence number global rather than per-subject?

- A: Because different subjects refer to the same schema IDs: if we treated subjects as independent, they might both
  grab the same schema ID for different schema definitions.

See worked examples further down that include sequence numbers.

## I/O

### Reads

Reads are served symmetrically from any node.

High traffic read endpoints are usually served from memory, falling back to read from the topic only if the requested ID
is not found locally (i.e. the local ShardedStore is out of date).

These fast-read endpoints are:

- `GET /schemas/ids/{int: id}`
- `GET /subjects/(string: subject)/versions/(versionId: version)`
- `GET /subjects/(string: subject)/versions/(versionId: version)/schema`

Endpoints that generate lists require a read from the topic to ensure that the list they are returning is complete.
These are:
GET /schemas/ids/{int: id}/versions
(because while we might have the schema in memory, it may have new subject-version associations we haven't seen yet)

- `GET /subjects`
- `GET /subjects/(string: subject)/version`
- `POST /subjects/(string: subject)`
- `GET /subjects/(string: subject)/versions/{versionId: version}/referencedby`

Within a single node, only one read to the topic is in flight at at time (enforced by a semaphore) -- this serialization
of reads reflects the serial layout of the underlying store.

*Future: use a queue-of-waiters synchronisation mechanism to reduce number of topic reads when many readers hit slow
endpoints concurrently within the same node*

*Future: if these endpoints turn out to be higher traffic than expected, introduce a config setting to make them
eventually consistent*

### Writes & deletes

As well as being subject to inter-node serialization via sequence numbers, writes are serialized within a node using a
semaphore. This simplifies the code, to avoid having rollback logic for projecting schema+version handle cases where
multiple writes are in flight & only some succeed.

*Future: if we find it necessary to

Writes are served symmetrically from any node, **but** it is advisable to send writes to the same node (doesn't matter
which)
as much as possible, to reduce the cost of synchronisation.

- `POST /subjects/(string: subject)/versions`

Soft deletions (without `permanent` parameter) are normal writes, which create new sequenced keys and include a flag in
the value to indicate that the named subject or version should be marked deleted.

- `DELETE /subjects/(string: subject)`
- `DELETE /subjects/(string: subject)/versions/(versionId: version)`

Permanent deletions render schemas unreadable, and are expected to free underlying storage. For us, that means emitting
tombstones to the keys used when writing the subject/version.

- `DELETE /subjects/(string: subject)?permanent=true`
- `DELETE /subjects/(string: subject)/versions/(versionId: version)?permanent=true`

### Examples

*Using shorthand key format of sequence-node-subject-version, where nodes are like A, B, C...*

#### Uncontended create & delete lifecycle

    # POST /subjects/foo
    1-A-foo-1 {schema}
    # POST /subjects/foo
    2-A-foo-2 {schema}
    # DELETE /subjects/foo
    3-A-foo-1 {deleted=true}
    4-A-foo-2 {deleted=true}
    # DELETE /subjects/foo?permanent=true
    1-A-foo-1 NULL
    1-A-foo-2 NULL
    3-A-foo-1 NULL
    4-A-foo-1 NULL

#### Contended writes (same subject+value)

    # POST A/subjects/foo
    1-A-foo-1 {schema}
    # Node A discovers they succeeded, returns schema=1 to caller

    # POST B/subjects/foo
    1-B-foo-1 {schema}
    # Node B re-reads, sees A's write, realizes value was already applied, returns schema=1 to caller

#### Contended writes (different subject)

    # POST A/subjects/foo
    1-A-foo-1 {schema}
    # Node A succeeds at offset 1.

    # POST B/subjects/bar
    1-B-foo-1 {schema}
    # Node B sees that its write landed at 'wrong' offset.  Re-reads.  Retries write.
    2-B-foo-1 {schema}
    # Node B succeeds at offset 2.

#### Contended hard deletes

    # DELETE A/subjects/foo?permanent=true
    1-A-foo-1 NULL

    # DELETE A/subjects/foo?permanent=true
    1-A-foo-1 NULL

    # Both nodes 'win'.
