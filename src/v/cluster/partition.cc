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
    auto stm_manager = _raft->log().stm_manager();
    if (is_id_allocator_topic(_raft->ntp())) {
        _id_allocator_stm = ss::make_lw_shared<raft::id_allocator_stm>(
          clusterlog, _raft.get(), config::shard_local_cfg());
    } else {
        if (_raft->log_config().is_collectable()) {
            _nop_stm = ss::make_lw_shared<raft::log_eviction_stm>(
              _raft.get(), clusterlog, stm_manager, _as);
        }
        if (config::shard_local_cfg().enable_idempotence.value()) {
            _seq_stm = ss::make_shared<seq_stm>(
              clusterlog, _raft.get(), config::shard_local_cfg());
            stm_manager->add_stm(_seq_stm);
        }
    }
}

ss::future<result<raft::replicate_result>> partition::replicate(
  model::record_batch_reader&& r, raft::replicate_options opts) {
    return _raft->replicate(std::move(r), std::move(opts));
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
partition::replicate(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    if (bid.has_idempotent()) {
        return _seq_stm->replicate(bid, std::move(r), std::move(opts));
    } else {
        return _raft->replicate(std::move(r), std::move(opts))
          .then([](result<raft::replicate_result> result) {
              if (result.has_value()) {
                  return checked<raft::replicate_result, kafka::error_code>(
                    result.value());
              } else {
                  return checked<raft::replicate_result, kafka::error_code>(
                    kafka::error_code::unknown_server_error);
              }
          });
    }
}

ss::future<> partition::start() {
    auto ntp = _raft->ntp();

    _probe.setup_metrics(ntp);

    auto f = _raft->start();

    if (is_id_allocator_topic(ntp)) {
        return f.then([this] { return _id_allocator_stm->start(); });
    } else if (_nop_stm) {
        f = f.then([this] { return _nop_stm->start(); });
    }

    if (_seq_stm) {
        f = f.then([this] { return _seq_stm->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    _as.request_abort();

    auto f = ss::now();

    if (_id_allocator_stm) {
        return _id_allocator_stm->stop();
    }

    if (_nop_stm) {
        f = _nop_stm->stop();
    }

    if (_seq_stm) {
        f = f.then([this] { return _seq_stm->stop(); });
    }

    // no state machine
    return f;
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
