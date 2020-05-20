#pragma once
#include "model/fundamental.h"
#include "raft/offset_monitor.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/btree_map.h>

namespace raft {

class consensus;

/**
 * Raft event notification manager.
 *
 * The raft event manager handles subscriptions to raft events, such as
 * leadership changes or reaching a specific commit index. The same or similar
 * notifications are available directly from the consensus module, but those
 * notifications are generally delivered under a lock and in the hot path. To
 * avoid introducing additional latency into raft as the number of subscribers
 * inreases, this manager receives direct notifications from raft where they
 * are then delivered asynchronously.
 */
class event_manager {
public:
    explicit event_manager(consensus* c) noexcept
      : _consensus(c) {}

    ss::future<> start();
    ss::future<> stop();

    /**
     * Wait until the commit index is greater than or equal to the provided
     * offset, or an abort has been requested through the provided abort source.
     */
    ss::future<>
    wait(model::offset, model::timeout_clock::time_point, ss::abort_source&);

    void notify_commit_index(model::offset);

private:
    consensus* _consensus;
    ss::gate _gate;
    ss::condition_variable _cond;
    offset_monitor _commit_index;
};

} // namespace raft
