#pragma once

#include "model/metadata.h"
#include "raft/consensus.h"
#include "seastarx.h"
#include "storage/types.h"

#include <seastar/core/semaphore.hh>

namespace raft {

/// A single-shot class. Utility method with state
/// Use with a lw_shared_ptr like so:
/// auto ptr = ss::make_lw_shared<replicate_entries_stm>(..);
/// return ptr->apply()
///            .then([ptr]{
///                 // wait in background.
///                (void)ptr->wait().finally([ptr]{});
///            });
///
/// Replicate STM implements following algorithm
///
///  Stop conditions:
///    1) leader commit_index > offset of an entry that was replicated
///       to the leader log (SUCCESS)
///    2) term has changed (FAILURE)
///    3) current node is not the leader (FAILURE)
///
///  Algorithm steps:
///    1) Append entry to leader log without flush
///    2) Dispatch append entries RPC calls to followers and in parallel flush
///       entries to leader disk
///    3) Wait for (1),(2) or (3)
///    4) When
///       ->(1) reply with success
///       ->(2) or (3) check if entry with given offset and term exists in log
///         if entry exists reply with success, reply with false otherwise
///
///
///   Wait is realized with condition variable that is only notified when commit
///   index change
///
///
///
///                            N1 (Leader)        +
///                            +-------+          |
///                        +-->| Flush |--------->+     OK
///                        |   +-------+          |     +----(1)----> SUCCESS
///                        |                      |     |
///      N1 (Leader)       |   N2                 |     |
/// +-------------------+  |   +--------+-------+ |     |
/// |Replicate |Append  |--+-->+ Append | Flush |-+---->+
/// +-------------------+  |   +--------+-------+ |     |
///                     |  |                      |     |
///                     |  |   N3                 |     |
///                     |  |   +--------+-------+ |     +-(2)-(3)---> FAILURE
///                     |  +-->| Append | Flush |-+     ERR
///                     |      +--------+-------+ |
///                     |                         +
///                     v               Wait for (1) or (2)
///         Store entry offset & term

class replicate_entries_stm {
public:
    replicate_entries_stm(consensus*, append_entries_request);
    ~replicate_entries_stm();

    /// caller have to pass _op_sem semaphore units, the apply call will do the
    /// fine grained locking on behalf of the caller
    ss::future<result<replicate_result>> apply(ss::semaphore_units<>);

    /// waits for the remaining background futures
    ss::future<> wait();

private:
    ss::future<append_entries_request> share_request();

    ss::future<> dispatch_one(model::node_id);
    ss::future<result<append_entries_reply>>
      dispatch_single_retry(model::node_id);
    ss::future<result<append_entries_reply>>
      do_dispatch_one(model::node_id, append_entries_request);

    ss::future<result<append_entries_reply>>
    send_append_entries_request(model::node_id n, append_entries_request req);
    result<replicate_result> process_result(model::offset, model::term_id);
    bool is_follower_recovering(model::node_id);
    /// This append will happen under the lock
    ss::future<storage::append_result> append_to_self();
    consensus* _ptr;
    /// we keep a copy around until we finish the retries
    append_entries_request _req;
    ss::semaphore _share_sem;
    ss::semaphore _dispatch_sem{0};
    ss::gate _req_bg;
    raft_ctx_log _ctxlog;
};

} // namespace raft
