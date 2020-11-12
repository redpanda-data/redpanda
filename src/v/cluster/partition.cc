// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"

#include "cluster/logger.h"
#include "prometheus/prometheus_sanitize.h"

namespace cluster {

partition::partition(consensus_ptr r)
  : _raft(r)
  , _probe(*this) {
    if (_raft->log_config().is_collectable()) {
        _nop_stm = std::make_unique<raft::log_eviction_stm>(
          _raft.get(), clusterlog, _as);
    }
}

ss::future<> partition::start() {
    _probe.setup_metrics(_raft->ntp());

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
