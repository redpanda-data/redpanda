#include "cluster/partition.h"

#include "cluster/logger.h"

namespace cluster {

partition::partition(consensus_ptr r)
  : _raft(r) {
    if (_raft->log_config().is_collectable()) {
        _nop_stm = std::make_unique<raft::log_eviction_stm>(
          _raft.get(), clusterlog, _as);
    }
}

ss::future<> partition::start() {
    auto f = _raft->start();

    if (_nop_stm != nullptr) {
        return f.then([this] { return _nop_stm->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    _as.request_abort();
    // no state machine
    if (_nop_stm == nullptr) {
        return ss::now();
    }

    return _nop_stm->stop();
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(model::timestamp t, ss::io_priority_class p) {
    storage::timequery_config cfg(t, _raft->committed_offset(), p);
    return _raft->timequery(cfg);
}

std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
} // namespace cluster
