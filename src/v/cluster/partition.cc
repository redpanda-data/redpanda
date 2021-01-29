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
#include "config/configuration.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"

namespace cluster {

static bool is_id_allocator_topic(model::ntp ntp) {
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::id_allocator_topic;
}

partition::partition(consensus_ptr r)
  : _raft(r)
  , _probe(*this) {
    if (is_id_allocator_topic(_raft->ntp())) {
        _id_allocator_stm = std::make_unique<raft::id_allocator_stm>(
          clusterlog, _raft.get(), config::shard_local_cfg());
    } else if (_raft->log_config().is_collectable()) {
        _nop_stm = std::make_unique<raft::log_eviction_stm>(
          _raft.get(),
          clusterlog,
          ss::make_lw_shared<storage::stm_manager>(),
          _as);
    }
}

ss::future<> partition::start() {
    auto ntp = _raft->ntp();

    _probe.setup_metrics(ntp);

    auto f = _raft->start();

    if (is_id_allocator_topic(ntp)) {
        return f.then([this] { return _id_allocator_stm->start(); });
    } else if (_nop_stm != nullptr) {
        return f.then([this] { return _nop_stm->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    _as.request_abort();

    if (_id_allocator_stm != nullptr) {
        return _id_allocator_stm->stop();
    }

    if (_nop_stm != nullptr) {
        return _nop_stm->stop();
    }

    // no state machine
    return ss::now();
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
