#include "cluster/partition.h"

namespace cluster {

partition::partition(consensus_ptr r)
  : _raft(r) {}

partition::partition(consensus_ptr r, raft::log_eviction_stm stm)
  : _raft(r)
  , _nop_stm(std::make_unique<raft::log_eviction_stm>(std::move(stm))) {}

ss::future<> partition::start() {
    auto f = _raft->start();

    if (_nop_stm != nullptr) {
        return f.then([this] { return _nop_stm->start(); });
    }

    return f;
}
ss::future<> partition::stop() {
    auto f = _raft->stop();
    // no state machine
    if (_nop_stm == nullptr) {
        return f;
    }

    return f.then([this] { return _nop_stm->stop(); });
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
