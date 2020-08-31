- Feature Name: Compacted log recovery
- Status: draft
- Start Date: 2020-07-13
- Authors: michal
- Issue: 

# Executive Summary

In raft log maintained by the consensus algorithm is gap less. This assumption
allow nodes to reason about log correctness checking only the last log entry. This
assumption is valid for non compacted Redpanda log instances. Compaction
algorithm introduces intentional gaps in the log to remove outdated values.
Additionally the compaction is not coordinated on all nodes and can be in
different state at each replica.

Compacted log record batches (gaps) makes Raft recovery impossible, as follower
is unable to check if it log is the same as the leader one.
Moreover in Redpanda offsets are assigned to batches when they are appended
to disk. Having gaps in the log would lead to incorrect offset assignment as
even though there are gaps in the offsets follower will assign consecutive
offsets.

## Example

```plain
    Leader log:   [0,10][11,20]<--gap from 21 to 32-->[33,40]

    Follower log: [0,10]
```

When offsets are send to follower the offset assignment happen in its
log_appender instance. The offsets are assigned monotonically starting
from 0 without gaps.

During normal recovery procedure the follower would have the following offsets
assigned:

```plain
    [0,10][11,20][21,28]
```

this is incorrect as batch `[21,28]` has different offsets `[33,40]` at the 
leader.

### Legend

```plain

[base_offset,last_offset] - single batch

<-- --> - gap
```

## What is being proposed

The proposal is to use the 'ghost batches' indicating intentional gaps in the
log. The ghost batches will not be persisted to disk but will be used in raft
recovery mechanism to force correct offset assignment and provide gap-less log
for Raft follower nodes.

# Guide-level explanation

## What use cases does it support?

Proposed solution supports raft recovery for compacted logs not comprising the
Raft protocol consistency guarantees. The solution does not introduce additional
storage cost introducing only small overhead in append entries request
(57 bytes per one gap).

## What is the expected outcome?

Expected outcome of the proposed solution is to have mechanism allowing Raft to
work with compacted logs without comprising any of it safety guarantees.

## Introducing new named concepts

Proposed solution defines new `model::record_batch_type` being representation
of 'ghost' batch denoting an intentional gap left after compacting the log.

## Detailed design - What needs to change to get there

### `raft::recovery_stm`

In raft recovery state machine record batches read for the follower recovery are
materialized in memory as `ss::circular_buffer<model::record_batch>`.
The recovery STM iterates over materialized records buffer and inserts
`storage::ghost_record_batch_type` into places where gap in the log occur.

```c++
  // fill batches collection with gaps
  auto gap_filled_batches = details::fill_gaps(
              start_offset, std::move(batches));
  _base_batch_offset = gap_filled_batches.begin()->base_offset();
  _last_batch_offset = gap_filled_batches.back().last_offset();

  auto f_reader = model::make_foreign_record_batch_reader(
                      model::make_memory_record_batch_reader(
                                         std::move(gap_filled_batches)));
  return replicate(std::move(f_reader));
```

### `storage::disk_log_appender`

Disk log appender use ghost record batches for correct offsets accounting but
do not persists them to disk.

```c++
ss::future<ss::stop_iteration>
disk_log_appender::append_batch_to_segment(const model::record_batch& batch) {
    // ghost batch handling, it doesn't happen often so we can use unlikely
    if (unlikely(batch.header().type == storage::ghost_record_batch_type)) {
        _idx = batch.last_offset() + model::offset(1); // next base offset
        _last_offset = batch.last_offset();
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    return _seg->append(batch).then([this](append_result r) {
        _idx = r.last_offset + model::offset(1); // next base offset
        _byte_size += r.byte_size;
        // do not track base_offset, only the last one
        _last_offset = r.last_offset;
        auto& p = _log.get_probe();
        p.add_bytes_written(r.byte_size);
        p.batch_written();
        // substract the bytes from the append
        // take the min because _bytes_left_in_segment is optimistic
        _bytes_left_in_segment -= std::min(_bytes_left_in_segment, r.byte_size);
        return ss::stop_iteration::no;
    });
```

### Term handling

The ghost batches may fill gap that spans across batches coming from different 
terms.

Consider an example:

```plain
              |- term 1 --|                       |- term 3 ...
Leader log:   [0,10][11,20]<--gap from 21 to 32-->[33,40]
```

In this example batches in range `[0,20]` come from term `1` while batches in
range `[33,...]` from term 3. We are unable to determine what was the term of
that were removed during compaction process.
In order to guarantee correctness we assume that the gap has term of batch that
immediately follows it in the log. (In current example ghost batch in range
`[21,32]` would have term `3`). An alternative would be to use term of a batch
immediately preceding the gap in log, however the ghost batch may be the first
batch in the range of batches, then it term couldn't be established.
This problem do not exists for the proposed approach as the gap will never exist
as a last element in a series of batches delivered for recovery.

## Drawbacks

The main drawback of the solution is an overhead introduced by gap filling
batches i.e. 57 bytes per gap. In real life scenarios the overhead will be
minimized as some large gaps will require only single gap representing batch even
though many batches were compacted.

## Rationale and Alternatives

More complicated alternative would be to force follower assigned offsets with
those coming from the leader. The main flaw of this solution is the fact that it
is hard to correctly reason about it's impact on raft protocol safety as it
diverge from the originally published Raft algorithm.

## Unresolved questions
