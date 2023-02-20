- Feature Name: Raft lazy flush
- Status: completed
- Start Date: 2020-04-21
- Authors: Michal
- Issue: ch233

# Executive Summary

This change proposing not flushing during recovery or during non-quorum writes.

## Why (short reason)

In redpanda we define consistency levels. For now Raft differentiate the
`quorum_ack` from `leader_ack` and `no_ack` (both of them are treated the same
way). The `quorum_ack` use classic Raft algorithm to replicate the changes
across the cluster. For `leader_ack` and `no_ack` we return success after
successful append to the leader log and we relay on raft recovery to replicate
data to follower logs. This makes recovery to be first class citizen of our Raft
algorithm implementation which is different to the one used in classic
implementation where recovery is exceptional situation and happen only after
failures. This makes the need to make recovery, being an async replication, fast.

## How

By default classic Raft algorithm implementations rely on the fact that entries
appended to follower logs are non volatile (flushed). Thanks to our log
implementation we can reliably track both the committed
(flushed and nonvolatile offset) and dirty (not flushed offset).
Raft implementation can then delay flushing follower log while still keep
tracking what entries are safe to be committed.

## Impact

Thanks to the proposed solution redpanda raft implementation will efficiently
support both the `quorum_ack` and relaxed consistency models.

# Reference-level explanation

## Current implementation

In current raft algorithm implementation all raft domain indexes are based
on `storage::log::dirty_offset`.

### Replicate algorithm - `consistency_level::quorum_ack`

Stop conditions:

  1. leader commit_index > offset of an entry that was replicated to
     the leader log (SUCCESS)
  2. term has changed (FAILURE)
  3. current node is not the leader (FAILURE)

Algorithm steps:

  1. Append entry to leader log without flush
  2. Dispatch append entries RPC calls to followers and in parallel flush
     entries to leader disk
  3. Wait for (1),(2) or (3)
  4. When
     ->(1) reply with success
     ->(2) or (3) check if entry with given offset and term exists in log
       if entry exists reply with success, reply with false otherwise

                           N1 (Leader)        +
                           +-------+          |
                       +-->| Flush |--------->+     OK
                       |   +-------+          |     +----(1)----> SUCCESS
                       |                      |     |
     N1 (Leader)       |   N2                 |     |
+-------------------+  |   +--------+-------+ |     |
|Replicate |Append  |--+-->+ Append | Flush |-+---->+
+-------------------+  |   +--------+-------+ |     |
                    |  |                      |     |
                    |  |   N3                 |     |
                    |  |   +--------+-------+ |     +-(2)-(3)---> FAILURE
                    |  +-->| Append | Flush |-+     ERR
                    |      +--------+-------+ |
                    |                         +
                    v               Wait for (1) or (2)
        Store entry offset & term

This algorithm indicates that on the followers log is flushed before response is
returned to the leader thanks to that leader can use the returned `last_log_index`
to establish the latest offset that has been safely (persistently) replicated on
 majority of nodes and using this information update the `commit_index`.

### Replicate algorithm - `consistency_level::leader_ack` and `consistency_level::no_ack`

Algorithm steps:

  1. Append entry to leader log without flush
  2. If append was successful reply with (1) success,
     reply with an error (2) otherwise

                           OK
                            +----(1)----> SUCCESS
                            |
     N1 (Leader)            |
+-------------------+       |
|Replicate |Append  |------>+
+-------------------+       |
                            |
                            |
                            +----(2)----> FAILURE
                           ERR

Follower replication is done in async manner as leader log is ahead of the
follower and recovery is triggered.

Flushing strategy:

- Leader log is flushed periodically (currently using vote_timer)
- Follower log is flushed on each append entry coming from recovery

### Recovery algorithm

`match_index` - greatest index where leader entry term
                is equal to follower entry term

1. Start recovery if follower append_entries/heartbeat returned failure
   and follower `match_index` is smaller than leader log `dirty_offset`
2. If follower `last_log_index` is greater than leader `last_log_index`
   establish new `match_index` by sending `append_entries` moving back
   one batch at a time. Follower will reply with success and this reply
   `last_log_index` is new `match_index`
3. Read up to 32KB of record_batches and send them as `append_entries_request`
   to follower until its `match_index` isn't equal to leader last log index

## Problems with current approach

Recovery happens in small batches (either 32KB or one batch, whichever is bigger)
and as the `append_entries` request is sent to the follower current flushing
policy force to it flush the underlying log before responding to the leader.
Follower has to flush the log to achieve correctness as leader will use
returned `last_log_index` to update its `commit_index`.

## Relation between committed and dirty offset

                    Committed batches
             +-----------------------------+
             |                             |
             v                             v
             +-------------------------------------------------+
             |Batch #1 |Batch #2 |Batch #3 |Batch #4 |Batch #5 |
             +-------------------------------------------------+
Offset       0         2         5         7         9         12
                                           ^                   ^
                                           |                   |
                                           +-------------------+
                                               Dirty batches
Committed offset: 7
Dirty offset: 12

Proposed change relies on fact that batches that were already committed
are non volatile while the one that are 'dirty' are appended to the log already
but can be lost in case of failure.

### Raft related observations

- Leader can only commit (update `commit_index`) those entries which are
  flushed on the majority of nodes
- In order to successfully append entries to the log followers
  have to receive entries with offset that follows its log `dirty_offset`

## Proposed solution

The relation described in previous point is a key to proposed solution.
While replicating with `quorum_ack` raft can only respond to caller when entry
was committed by raft protocol in order to minimize latency we want to do it
in one roundtrip round. For relaxed consistency levels we do not
require majority to persistently store replicated entries before
returning to the caller. In order to achieve performance gain while replicating
entries during recovery we can postpone updating `commit_index` on the leader
and allow follower not to flush on every `append_entries` request coming
from the leader. This will speed up sending 32 KB recovery chunks as leader
will not have to wait for follower to flush before sending next chunk. Leader
`commit_index` will eventually be updated only after it will receive information
from follower that it flushed its log up to the certain point. We do not have to
set `flush_after_append` even for `quorum_ack` recovery as heartbeats will
contain followers committed and dirty index so leader can use them to update
followers state.

## Detailed design - What needs to change to get there

### Changes in leader

Leader has to be able to track both the flushed and dirty offsets
from the followers. So it can use the dirty offset to establish the latest
follower index that matches leader in order to trigger recovery if required.
The committed offset on the other hand is used to establish the offset that was
persistently replicate across majority of nodes.
In order to make it possible new fields have to be added to
`follower_index_metadata`:

    ```c++
        struct follower_index_metadata {
            explicit follower_index_metadata(model::node_id node)
            : node_id(node) {}
            model::node_id node_id;
            // index of last known log for this follower
            model::offset last_committed_log_index; // <----------- New field
            // index of last not flushed offset
            model::offset last_dirty_log_index; // <----------- New field
            // index of log for which leader and follower logs matches
            model::offset match_index;
            // Used to establish index persistently replicated by majority
            constexpr model::offset match_committed_index() const {
                return std::min(last_committed_log_index, match_index);
            }
            // next index to send to this follower
            model::offset next_index;
            // timestamp of last append_entries_rpc call
            clock_type::time_point last_sent_append_entries_req_timestamp;
            uint64_t failed_appends{0};
            bool is_learner = false;
            bool is_recovering = false;
            // indicates if leader has active connection to this node
            bool disconnected = false;
        };
    ```
The addition of `last_committed_log_index` and `last_dirty_log_index`
leader can track both flushed and dirty offsets of followers.

### Changes in follower

In order to allow leader to track both committed and dirty offsets have to be
send in `append_entries_reply`. This will be done in both the heartbeat and
`append_entries` containing data.

    ```c++
    struct append_entries_reply {
        enum class status : uint8_t { success, failure, group_unavailable };
        /// \brief callee's node_id; work-around for batched heartbeats
        model::node_id node_id;
        group_id group;
        /// \brief callee's term, for the caller to upate itself
        model::term_id term;
        /// \brief The recipient's last log index after it applied changes to
        /// the log. This is used to speed up finding the correct value for the
        /// nextIndex with a follower that is far behind a leader
        model::offset last_committed_log_index; // <----------- New field
        model::offset last_dirty_log_index; // <----------- New field
        /// \brief did the rpc succeed or not
        status result = status::failure;
    };
    ```

Additionally leader has to be able to control
if follower should flush while processing `append_entries_request`.

    ```c++
    struct append_entries_request {
        using flush_after_append = ss::bool_class<struct flush_after_append_tag>;

        append_entries_request(
        model::node_id i,
        protocol_metadata m,
        model::record_batch_reader r,
        flush_after_append f = flush_after_append::yes) noexcept
        : node_id(i)
        , meta(m)
        , batches(std::move(r))
        , flush(f){};
        ~append_entries_request() noexcept = default;
        append_entries_request(const append_entries_request&) = delete;
        append_entries_request& operator=(const append_entries_request&) = delete;
        append_entries_request(append_entries_request&&) noexcept = default;
        append_entries_request&
        operator=(append_entries_request&&) noexcept = default;

        model::node_id node_id;
        protocol_metadata meta;
        model::record_batch_reader batches;
        flush_after_append flush; // <----------- New field
        static append_entries_request make_foreign(append_entries_request&& req) {
            return append_entries_request(
            req.node_id,
            std::move(req.meta),
            model::make_foreign_record_batch_reader(std::move(req.batches)),
            req.flush);
        }
    };
    ```

## Drawbacks

- When recovering followers for those partitions that use `quorum_ack` leader will
  have to wait before updating `commit_index` until it will eventually get
  information from the follower that entries were flushed.

- Adds additional 8 bytes per group to heartbeat response.

## Rationale and Alternatives

- There was a proposed alternative to use consistency level as a part of
  `append_entries_request` to let the follower know if it have to flush
  immediately. This would require persisting this information in log as entries
  send during recovery are read directly from the log/cache. Additionally it has
  no additional benefit as recovery can work in the same way for `quorum_ack`
  and relaxed consistency level
