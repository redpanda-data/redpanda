#pragma once
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

#include <absl/container/btree_map.h>

namespace raft {

/**
 * Offset monitor.
 *
 * Utility for manging waiters based on a threshold offset. Supports multiple
 * waiters on the same offset, as well as timeout and abort source methods of
 * aborting a wait.
 */
class offset_monitor {
public:
    /**
     * Exception used to indicate an aborted wait, either from a requested abort
     * via an abort source or because a timeout occurred.
     */
    class wait_aborted final : public std::exception {
    public:
        virtual const char* what() const noexcept final {
            return "offset monitor wait aborted";
        }
    };

    /**
     * Exisiting waiters receive wait_aborted exception.
     */
    void stop();

    /**
     * Wait until at least the given offset has been notified, a timeout
     * occurs, or an abort has been requested.
     */
    ss::future<> wait(
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    /**
     * Returns true if there are no waiters.
     */
    bool empty() const { return _waiters.empty(); }

    /**
     * Notify waiters of an offset.
     */
    void notify(model::offset);

private:
    struct waiter {
        offset_monitor* mon;
        ss::promise<> done;
        ss::timer<model::timeout_clock> timer;
        ss::abort_source::subscription sub;

        waiter(
          offset_monitor*,
          model::timeout_clock::time_point,
          std::optional<std::reference_wrapper<ss::abort_source>>);

        void handle_abort();
    };

    friend waiter;

    using waiters_type
      = absl::btree_multimap<model::offset, std::unique_ptr<waiter>>;

    waiters_type _waiters;
};

} // namespace raft
