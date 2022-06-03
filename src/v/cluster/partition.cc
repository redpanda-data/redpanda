// Copyright 2020 Redpanda Data, Inc.
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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/types.h"

namespace cluster {

static bool is_id_allocator_topic(model::ntp ntp) {
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::id_allocator_topic;
}

static bool is_tx_manager_topic(const model::ntp& ntp) {
    return ntp == model::tx_manager_ntp;
}

partition::partition(
  consensus_ptr r,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_storage_cache)
  : _raft(r)
  , _probe(std::make_unique<replicated_partition_probe>(*this))
  , _probe_v2()
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _is_tx_enabled(config::shard_local_cfg().enable_transactions.value())
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value()) {
    auto stm_manager = _raft->log().stm_manager();

    if (is_id_allocator_topic(_raft->ntp())) {
        _id_allocator_stm = ss::make_shared<cluster::id_allocator_stm>(
          clusterlog, _raft.get());
    } else if (is_tx_manager_topic(_raft->ntp())) {
        if (_raft->log_config().is_collectable()) {
            _log_eviction_stm = ss::make_lw_shared<raft::log_eviction_stm>(
              _raft.get(), clusterlog, stm_manager, _as);
        }

        if (_is_tx_enabled) {
            _tm_stm = ss::make_shared<cluster::tm_stm>(clusterlog, _raft.get());
            stm_manager->add_stm(_tm_stm);
        }
    } else {
        if (_raft->log_config().is_collectable()) {
            _log_eviction_stm = ss::make_lw_shared<raft::log_eviction_stm>(
              _raft.get(), clusterlog, stm_manager, _as);
        }
        const model::topic_namespace tp_ns(
          _raft->ntp().ns, _raft->ntp().tp.topic);
        bool is_group_ntp = tp_ns == model::kafka_group_nt
                            || tp_ns == model::kafka_consumer_offsets_nt;

        bool has_rm_stm = (_is_tx_enabled || _is_idempotence_enabled)
                          && model::controller_ntp != _raft->ntp()
                          && !is_group_ntp;

        if (has_rm_stm) {
            _rm_stm = ss::make_shared<cluster::rm_stm>(
              clusterlog, _raft.get(), _tx_gateway_frontend);
            stm_manager->add_stm(_rm_stm);
        }

        if (
          config::shard_local_cfg().cloud_storage_enabled()
          && cloud_storage_api.local_is_initialized()
          && _raft->ntp().ns == model::kafka_namespace) {
            _archival_meta_stm
              = ss::make_shared<cluster::archival_metadata_stm>(
                _raft.get(), cloud_storage_api.local(), clusterlog);
            stm_manager->add_stm(_archival_meta_stm);

            if (cloud_storage_cache.local_is_initialized()) {
                auto bucket
                  = config::shard_local_cfg().cloud_storage_bucket.value();
                if (!bucket) {
                    throw std::runtime_error{
                      "configuration property cloud_storage_bucket is not set"};
                }

                _cloud_storage_partition
                  = ss::make_lw_shared<cloud_storage::remote_partition>(
                    _archival_meta_stm->manifest(),
                    cloud_storage_api.local(),
                    cloud_storage_cache.local(),
                    s3::bucket_name{*bucket});
            }
        }
    }
}

ss::future<result<raft::replicate_result>> partition::replicate(
  model::record_batch_reader&& r, raft::replicate_options opts) {
    return _raft->replicate(std::move(r), opts);
}

raft::replicate_stages partition::replicate_in_stages(
  model::record_batch_reader&& r, raft::replicate_options opts) {
    return _raft->replicate_in_stages(std::move(r), opts);
}

ss::future<result<raft::replicate_result>> partition::replicate(
  model::term_id term,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    return _raft->replicate(term, std::move(r), opts);
}

ss::shared_ptr<cluster::rm_stm> partition::rm_stm() {
    if (!_rm_stm) {
        if (!_is_tx_enabled && !_is_idempotence_enabled) {
            vlog(
              clusterlog.error,
              "Can't process transactional and idempotent requests to {}. The "
              "feature is disabled.",
              _raft->ntp());
        } else {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotency and transactional "
              "processing.",
              _raft->ntp());
        }
    }
    return _rm_stm;
}

raft::replicate_stages partition::replicate_in_stages(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    if (bid.is_transactional) {
        if (!_is_tx_enabled) {
            vlog(
              clusterlog.error,
              "Can't process a transactional request to {}. Transactional "
              "processing isn't enabled.",
              _raft->ntp());
            return raft::replicate_stages(raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support transactional processing.",
              _raft->ntp());
            return raft::replicate_stages(raft::errc::timeout);
        }
    }

    if (bid.has_idempotent()) {
        if (!_is_idempotence_enabled) {
            vlog(
              clusterlog.error,
              "Can't process an idempotent request to {}. Idempotency isn't "
              "enabled.",
              _raft->ntp());
            return raft::replicate_stages(raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotency.",
              _raft->ntp());
            return raft::replicate_stages(raft::errc::timeout);
        }
    }

    if (_rm_stm) {
        ss::promise<> p;
        auto f = p.get_future();
        auto replicate_finished
          = _rm_stm->replicate(bid, std::move(r), opts)
              .then(
                [p = std::move(p)](result<raft::replicate_result> res) mutable {
                    p.set_value();
                    return res;
                });
        return raft::replicate_stages(
          std::move(f), std::move(replicate_finished));

    } else {
        return _raft->replicate_in_stages(std::move(r), opts);
    }
}

ss::future<result<raft::replicate_result>> partition::replicate(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    if (bid.is_transactional) {
        if (!_is_tx_enabled) {
            vlog(
              clusterlog.error,
              "Can't process a transactional request to {}. Transactional "
              "processing isn't enabled.",
              _raft->ntp());
            return ss::make_ready_future<result<raft::replicate_result>>(
              raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support transactional processing.",
              _raft->ntp());
            return ss::make_ready_future<result<raft::replicate_result>>(
              raft::errc::timeout);
        }
    }

    if (bid.has_idempotent()) {
        if (!_is_idempotence_enabled) {
            vlog(
              clusterlog.error,
              "Can't process an idempotent request to {}. Idempotency isn't "
              "enabled.",
              _raft->ntp());
            return ss::make_ready_future<result<raft::replicate_result>>(
              raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotency.",
              _raft->ntp());
            return ss::make_ready_future<result<raft::replicate_result>>(
              raft::errc::timeout);
        }
    }

    if (_rm_stm) {
        return _rm_stm->replicate(bid, std::move(r), opts);
    } else {
        return _raft->replicate(std::move(r), opts);
    }
}

ss::future<> partition::start() {
    auto ntp = _raft->ntp();

    _probe.setup_metrics(ntp);
    _probe_v2.setup_metrics(ntp);

    auto f = _raft->start();

    if (is_id_allocator_topic(ntp)) {
        return f.then([this] { return _id_allocator_stm->start(); });
    } else if (_log_eviction_stm) {
        f = f.then([this] { return _log_eviction_stm->start(); });
    }

    if (_rm_stm) {
        f = f.then([this] { return _rm_stm->start(); });
    }

    if (_tm_stm) {
        f = f.then([this] { return _tm_stm->start(); });
    }

    if (_archival_meta_stm) {
        f = f.then([this] { return _archival_meta_stm->start(); });
    }

    if (_cloud_storage_partition) {
        f = f.then([this] { return _cloud_storage_partition->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    _as.request_abort();

    auto f = ss::now();

    if (_id_allocator_stm) {
        return _id_allocator_stm->stop();
    }

    if (_log_eviction_stm) {
        f = _log_eviction_stm->stop();
    }

    if (_rm_stm) {
        f = f.then([this] { return _rm_stm->stop(); });
    }

    if (_tm_stm) {
        f = f.then([this] { return _tm_stm->stop(); });
    }

    if (_archival_meta_stm) {
        f = f.then([this] { return _archival_meta_stm->stop(); });
    }

    if (_cloud_storage_partition) {
        f = f.then([this] { return _cloud_storage_partition->stop(); });
    }

    // no state machine
    return f;
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(storage::timequery_config cfg) {
    return _raft->timequery(cfg);
}

ss::future<> partition::update_configuration(topic_properties properties) {
    return _raft->log().update_configuration(
      properties.get_ntp_cfg_overrides());
}

std::optional<model::offset>
partition::get_term_last_offset(model::term_id term) const {
    auto o = _raft->log().get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next leader
    // epoch
    return model::next_offset(*o);
}

std::optional<model::offset>
partition::get_cloud_term_last_offset(model::term_id term) const {
    auto o = _cloud_storage_partition->get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next leader
    // epoch
    return model::next_offset(*o);
}
std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
} // namespace cluster
