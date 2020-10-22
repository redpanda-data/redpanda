#pragma once
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "outcome.h"
#include "raft/offset_monitor.h"
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

namespace raft {

class consensus;

/**
 * State machine adapter.
 *
 * TODO: the consensus pointer should be set in the constructor once we have
 * managed to fully remove consensus setup from the controller.
 *
 * Usage
 * =====
 *
 * A state machine implementation should inherit from this class:
 *
 *   class kv_store : raft::state_machine {
 *     std::map<std::string, std::string> db;
 *     ss::future<> apply(model::record_batch batch) override {...}
 *   };
 *
 * The state machine should be started after it is ready to receive batches and
 * apply them asynchronously from the apply upcall.
 *
 *     ss::future<> apply(model::record_batch batch) override {
 *       db_update(batch);
 *     }
 *
 * The state machine tracks which batches have been applied. Use the `wait`
 * primitive to wait until a particular log offset has been applied to the state
 * machine.
 */
class state_machine {
public:
    static constexpr model::record_batch_type checkpoint_batch_type{5};
    state_machine(consensus*, ss::logger& log, ss::io_priority_class io_prio);

    // start after ready to receive batches through apply upcall.
    ss::future<> start();

    ss::future<> stop();

    // wait until at least offset is applied to state machine
    ss::future<> wait(model::offset, model::timeout_clock::time_point);

    /**
     * This must be implemented by the state machine. The state machine should
     * replay this batch and return a completed future. If an exceptional future
     * is returned an error is logged and the same batch will be applied again.
     */
    virtual ss::future<> apply(model::record_batch) = 0;
    /**
     * Return last applied offset established when STM starts. This can be used
     * to wait for the entries to be applied when STM is starting.
     */
    model::offset bootstrap_last_applied() const;
    /**
     * Store last applied offset. If an offset is persisted it will be used by
     * consensus instance underlying this state machine to recovery committed
     * index on startup
     */
    ss::future<> write_last_applied(model::offset);

    ss::future<result<replicate_result>>
      quorum_write_empty_batch(model::timeout_clock::time_point);

private:
    class batch_applicator {
    public:
        explicit batch_applicator(state_machine*);
        ss::future<ss::stop_iteration> operator()(model::record_batch);
        model::offset end_of_stream() const { return _last_applied; }

    private:
        state_machine* _machine;
        model::offset _last_applied;
    };

    friend batch_applicator;

    ss::future<> apply();
    bool stop_batch_applicator();

    consensus* _raft;
    ss::io_priority_class _io_prio;
    ss::logger& _log;
    offset_monitor _waiters;
    model::offset _next;
    ss::abort_source _as;
    ss::gate _gate;
    model::offset _bootstrap_last_applied;
};

} // namespace raft
