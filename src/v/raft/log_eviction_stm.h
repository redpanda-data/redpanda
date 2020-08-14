#pragma once
#include "seastarx.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

namespace raft {

class consensus;

/**
 * Responsible for taking snapshots triggered by underlying log segments
 * eviction
 */
class log_eviction_stm {
public:
    log_eviction_stm(consensus*, ss::logger&);

    ss::future<> start();

    ss::future<> stop();

private:
    ss::future<> handle_deletion_notification(model::offset);
    void monitor_log_eviction();

    consensus* _raft;
    ss::logger& _logger;
    ss::gate _gate;
    ss::abort_source _as;
    model::offset _previous_eviction_offset;
};

} // namespace raft
