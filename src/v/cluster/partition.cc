// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"

#include "archival/ntp_archiver_service.h"
#include "archival/upload_housekeeping_service.h"
#include "cloud_storage/remote_partition.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/types.h"

#include <seastar/util/defer.hh>

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
  ss::sharded<cloud_storage::cache>& cloud_storage_cache,
  ss::lw_shared_ptr<const archival::configuration> archival_conf,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::tm_stm_cache>& tm_stm_cache,
  ss::sharded<archival::upload_housekeeping_service>& upload_hks,
  config::binding<uint64_t> max_concurrent_producer_ids,
  std::optional<cloud_storage_clients::bucket_name> read_replica_bucket)
  : _raft(r)
  , _partition_mem_tracker(
      ss::make_shared<util::mem_tracker>(_raft->ntp().path()))
  , _probe(std::make_unique<replicated_partition_probe>(*this))
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _feature_table(feature_table)
  , _tm_stm_cache(tm_stm_cache)
  , _is_tx_enabled(config::shard_local_cfg().enable_transactions.value())
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value())
  , _archival_conf(archival_conf)
  , _cloud_storage_api(cloud_storage_api)
  , _upload_housekeeping(upload_hks) {
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
            _tm_stm = ss::make_shared<cluster::tm_stm>(
              clusterlog, _raft.get(), feature_table, _tm_stm_cache);
            stm_manager->add_stm(_tm_stm);
        }
    } else {
        if (_raft->log_config().is_collectable()) {
            _log_eviction_stm = ss::make_lw_shared<raft::log_eviction_stm>(
              _raft.get(), clusterlog, stm_manager, _as);
        }
        const model::topic_namespace tp_ns(
          _raft->ntp().ns, _raft->ntp().tp.topic);
        bool is_group_ntp = tp_ns == model::kafka_consumer_offsets_nt;

        bool has_rm_stm = (_is_tx_enabled || _is_idempotence_enabled)
                          && model::controller_ntp != _raft->ntp()
                          && !is_group_ntp;

        if (has_rm_stm) {
            _rm_stm = ss::make_shared<cluster::rm_stm>(
              clusterlog,
              _raft.get(),
              _tx_gateway_frontend,
              _feature_table,
              max_concurrent_producer_ids);
            stm_manager->add_stm(_rm_stm);
        }

        // Construct cloud_storage read path (remote_partition)
        if (
          config::shard_local_cfg().cloud_storage_enabled()
          && _cloud_storage_api.local_is_initialized()
          && _raft->ntp().ns == model::kafka_namespace) {
            _archival_meta_stm
              = ss::make_shared<cluster::archival_metadata_stm>(
                _raft.get(),
                _cloud_storage_api.local(),
                _feature_table.local(),
                clusterlog,
                _partition_mem_tracker);
            stm_manager->add_stm(_archival_meta_stm);

            if (cloud_storage_cache.local_is_initialized()) {
                const auto& bucket_config
                  = cloud_storage::configuration::get_bucket_config();
                auto bucket = bucket_config.value();
                if (
                  read_replica_bucket
                  && _raft->log_config().is_read_replica_mode_enabled()) {
                    vlog(
                      clusterlog.info,
                      "{} Remote topic bucket is {}",
                      _raft->ntp(),
                      read_replica_bucket);
                    // Override the bucket for read replicas
                    _read_replica_bucket = read_replica_bucket;
                    bucket = read_replica_bucket;
                }
                if (!bucket) {
                    throw std::runtime_error{fmt::format(
                      "configuration property {} is not set",
                      bucket_config.name())};
                }
                _cloud_storage_partition
                  = ss::make_shared<cloud_storage::remote_partition>(
                    _archival_meta_stm->manifest(),
                    _cloud_storage_api.local(),
                    cloud_storage_cache.local(),
                    cloud_storage_clients::bucket_name{*bucket});
            }
        }

        // Construct cloud_storage write path (ntp_archiver)
        maybe_construct_archiver();
    }
}

partition::~partition() {}

ss::future<std::vector<rm_stm::tx_range>> partition::aborted_transactions_cloud(
  const cloud_storage::offset_range& offsets) {
    return _cloud_storage_partition->aborted_transactions(offsets);
}

bool partition::is_remote_fetch_enabled() const {
    const auto& cfg = _raft->log_config();
    if (_feature_table.local().is_active(features::feature::cloud_retention)) {
        // Since 22.3, the ntp_config is authoritative.
        return cfg.is_remote_fetch_enabled();
    } else {
        // We are in the process of an upgrade: apply <22.3 behavior of acting
        // as if every partition has remote read enabled if the cluster
        // default is true.
        return cfg.is_remote_fetch_enabled()
               || config::shard_local_cfg().cloud_storage_enable_remote_read();
    }
}

bool partition::cloud_data_available() const {
    return static_cast<bool>(_cloud_storage_partition)
           && _cloud_storage_partition->is_data_available();
}

uint64_t partition::cloud_log_size() const {
    if (!cloud_data_available() || is_read_replica_mode_enabled()) {
        return 0;
    }

    return _cloud_storage_partition->cloud_log_size();
}

model::offset partition::start_cloud_offset() const {
    vassert(
      cloud_data_available(),
      "Method can only be called if cloud data is available, ntp: {}",
      _raft->ntp());
    return kafka::offset_cast(
      _cloud_storage_partition->first_uploaded_offset());
}

model::offset partition::last_cloud_offset() const {
    vassert(
      cloud_data_available(),
      "Method can only be called if cloud data is available, ntp: {}",
      _raft->ntp());
    return _cloud_storage_partition->last_uploaded_offset();
}

ss::future<storage::translating_reader> partition::make_cloud_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    vassert(
      cloud_data_available(),
      "Method can only be called if cloud data is available, ntp: {}",
      _raft->ntp());
    return _cloud_storage_partition->make_reader(config, deadline);
}

ss::future<result<kafka_result>> partition::replicate(
  model::record_batch_reader&& r, raft::replicate_options opts) {
    using ret_t = result<kafka_result>;
    auto res = co_await _raft->replicate(std::move(r), opts);
    if (!res) {
        co_return ret_t(res.error());
    }
    co_return ret_t(kafka_result{
      kafka::offset(_translator->from_log_offset(res.value().last_offset)())});
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

kafka_stages partition::replicate_in_stages(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    using ret_t = result<kafka_result>;
    if (bid.is_transactional) {
        if (!_is_tx_enabled) {
            vlog(
              clusterlog.error,
              "Can't process a transactional request to {}. Transactional "
              "processing isn't enabled.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support transactional processing.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    if (bid.has_idempotent()) {
        if (!_is_idempotence_enabled) {
            vlog(
              clusterlog.error,
              "Can't process an idempotent request to {}. Idempotency isn't "
              "enabled.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }

        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotency.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    if (_rm_stm) {
        return _rm_stm->replicate_in_stages(bid, std::move(r), opts);
    }

    auto res = _raft->replicate_in_stages(std::move(r), opts);
    auto replicate_finished = res.replicate_finished.then(
      [this](result<raft::replicate_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          auto old_offset = r.value().last_offset;
          auto new_offset = kafka::offset(
            _translator->from_log_offset(old_offset)());
          return ret_t(kafka_result{new_offset});
      });
    return kafka_stages(
      std::move(res.request_enqueued), std::move(replicate_finished));
}

ss::future<> partition::start() {
    auto ntp = _raft->ntp();

    _probe.setup_metrics(ntp);

    auto f = _raft->start().then(
      [this] { _translator = _raft->get_offset_translator_state(); });

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

    if (_archiver) {
        f = f.then([this] { return _archiver->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    auto partition_ntp = ntp();
    vlog(clusterlog.debug, "Stopping partition: {}", partition_ntp);
    _as.request_abort();

    if (_archiver) {
        _upload_housekeeping.local().deregister_jobs(
          _archiver->get_housekeeping_jobs());
        vlog(
          clusterlog.debug,
          "Stopping archiver on partition: {}",
          partition_ntp);
        co_await _archiver->stop();
    }

    if (_archival_meta_stm) {
        vlog(
          clusterlog.debug,
          "Stopping archival_meta_stm on partition: {}",
          partition_ntp);
        co_await _archival_meta_stm->stop();
    }

    if (_cloud_storage_partition) {
        vlog(
          clusterlog.debug,
          "Stopping cloud_storage_partition on partition: {}",
          partition_ntp);
        co_await _cloud_storage_partition->stop();
    }

    if (_id_allocator_stm) {
        vlog(
          clusterlog.debug,
          "Stopping id_allocator_stm on partition: {}",
          partition_ntp);
        co_await _id_allocator_stm->stop();
    }

    if (_log_eviction_stm) {
        vlog(
          clusterlog.debug,
          "Stopping log_eviction_stm on partition: {}",
          partition_ntp);
        co_await _log_eviction_stm->stop();
    }

    if (_rm_stm) {
        vlog(
          clusterlog.debug, "Stopping rm_stm on partition: {}", partition_ntp);
        co_await _rm_stm->stop();
    }

    if (_tm_stm) {
        vlog(
          clusterlog.debug, "Stopping tm_stm on partition: {}", partition_ntp);
        co_await _tm_stm->stop();
    }
    vlog(clusterlog.debug, "Stopped partition {}", partition_ntp);
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(storage::timequery_config cfg) {
    const bool is_read_replica
      = _raft->log_config().is_read_replica_mode_enabled();
    const bool query_before_raft_log_start = _raft->log().start_timestamp()
                                             > cfg.time;

    std::optional<storage::timequery_result> result;

    if (is_read_replica || query_before_raft_log_start) {
        // We have data in the remote partition, and all the data in the raft
        // log is ahead of the query timestamp or the topic is a read replica,
        // so proceed to query the remote partition to try and find the earliest
        // data that has timestamp >= the query time.
        co_return co_await cloud_storage_timequery(cfg);
    }

    result = co_await local_timequery(cfg);

    // It's possible that during the query, our local log was GCed, and the
    // result may not actually reflect the correct offset for this
    // partition. If the local log doesn't look like it can serve the query
    // anymore, query the remote log.
    if (_raft->log().start_timestamp() > cfg.time) {
        co_return co_await cloud_storage_timequery(cfg);
    } else {
        co_return result;
    }
}

ss::future<std::optional<storage::timequery_result>>
partition::cloud_storage_timequery(storage::timequery_config cfg) {
    const bool may_read_from_cloud
      = _cloud_storage_partition
        && _cloud_storage_partition->is_data_available();
    if (may_read_from_cloud) {
        // We have data in the remote partition, and all the data in the raft
        // log is ahead of the query timestamp or the topic is a read replica,
        // so proceed to query the remote partition to try and find the earliest
        // data that has timestamp >= the query time.
        vlog(
          clusterlog.debug,
          "timequery (cloud) {} t={} max_offset(k)={}",
          _raft->ntp(),
          cfg.time,
          cfg.max_offset);

        // remote_partition pre-translates offsets for us, so no call into
        // the offset translator here
        auto result = co_await _cloud_storage_partition->timequery(cfg);
        if (result) {
            vlog(
              clusterlog.debug,
              "timequery (cloud) {} t={} max_offset(r)={} result(r)={}",
              _raft->ntp(),
              cfg.time,
              cfg.max_offset,
              result->offset);
        }

        co_return result;
    }

    co_return std::nullopt;
}

ss::future<std::optional<storage::timequery_result>>
partition::local_timequery(storage::timequery_config cfg) {
    vlog(
      clusterlog.debug,
      "timequery (raft) {} t={} max_offset(k)={}",
      _raft->ntp(),
      cfg.time,
      cfg.max_offset);

    cfg.max_offset = _raft->get_offset_translator_state()->to_log_offset(
      cfg.max_offset);

    auto result = co_await _raft->timequery(cfg);
    if (result) {
        vlog(
          clusterlog.debug,
          "timequery (raft) {} t={} max_offset(r)={} result(r)={}",
          _raft->ntp(),
          cfg.time,
          cfg.max_offset,
          result->offset);
        result->offset = _raft->get_offset_translator_state()->from_log_offset(
          result->offset);
    }

    co_return result;
}

void partition::maybe_construct_archiver() {
    if (
      config::shard_local_cfg().cloud_storage_enabled()
      && _cloud_storage_api.local_is_initialized()
      && _raft->ntp().ns == model::kafka_namespace
      && _raft->log().config().is_archival_enabled()) {
        _archiver = std::make_unique<archival::ntp_archiver>(
          log().config(), _archival_conf, _cloud_storage_api.local(), *this);
        _upload_housekeeping.local().register_jobs(
          _archiver->get_housekeeping_jobs());
    }
}

uint64_t partition::non_log_disk_size_bytes() const {
    uint64_t non_log_disk_size = _raft->get_snapshot_size();
    if (_rm_stm) {
        non_log_disk_size += _rm_stm->get_snapshot_size();
    }
    if (_tm_stm) {
        non_log_disk_size += _tm_stm->get_snapshot_size();
    }
    if (_archival_meta_stm) {
        non_log_disk_size += _archival_meta_stm->get_snapshot_size();
    }
    if (_id_allocator_stm) {
        non_log_disk_size += _id_allocator_stm->get_snapshot_size();
    }
    return non_log_disk_size;
}

ss::future<> partition::update_configuration(topic_properties properties) {
    auto& old_ntp_config = _raft->log().config();
    auto new_ntp_config = properties.get_ntp_cfg_overrides();

    // Before applying change, consider whether it changes cloud storage
    // mode
    bool cloud_storage_changed = false;
    bool new_archival = new_ntp_config.shadow_indexing_mode
                        && model::is_archival_enabled(
                          new_ntp_config.shadow_indexing_mode.value());

    bool new_compaction_status
      = new_ntp_config.cleanup_policy_bitflags.has_value()
        && (new_ntp_config.cleanup_policy_bitflags.value()
            & model::cleanup_policy_bitflags::compaction)
             == model::cleanup_policy_bitflags::compaction;
    if (
      old_ntp_config.is_archival_enabled() != new_archival
      || old_ntp_config.is_read_replica_mode_enabled()
           != new_ntp_config.read_replica
      || old_ntp_config.is_compacted() != new_compaction_status) {
        cloud_storage_changed = true;
    }

    // Pass the configuration update into the storage layer
    co_await _raft->log().update_configuration(new_ntp_config);

    // If this partition's cloud storage mode changed, rebuild the archiver.
    // This must happen after raft update, because it reads raft's
    // ntp_config to decide whether to construct an archiver.
    if (cloud_storage_changed) {
        vlog(
          clusterlog.debug,
          "update_configuration[{}]: updating archiver for config {}",
          new_ntp_config,
          _raft->ntp());
        if (_archiver) {
            _upload_housekeeping.local().deregister_jobs(
              _archiver->get_housekeeping_jobs());
            co_await _archiver->stop();
            _archiver = nullptr;
        }
        maybe_construct_archiver();
        if (_archiver) {
            _archiver->notify_topic_config();
            co_await _archiver->start();
        }
    } else {
        vlog(
          clusterlog.trace,
          "update_configuration[{}]: no cloud storage change, archiver "
          "exists={}",
          _raft->ntp(),
          bool(_archiver));

        if (_archiver) {
            // Assume that a partition config may also mean a topic
            // configuration change.  This could be optimized by hooking
            // in separate updates from the controller when our topic
            // configuration changes.
            _archiver->notify_topic_config();
        }
    }
}

std::optional<model::offset>
partition::get_term_last_offset(model::term_id term) const {
    auto o = _raft->log().get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next
    // leader epoch
    return model::next_offset(*o);
}

std::optional<model::offset>
partition::get_cloud_term_last_offset(model::term_id term) const {
    auto o = _cloud_storage_partition->get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next
    // leader epoch
    return model::next_offset(kafka::offset_cast(*o));
}

ss::future<> partition::remove_persistent_state() {
    if (_rm_stm) {
        co_await _rm_stm->remove_persistent_state();
    }
    if (_tm_stm) {
        co_await _tm_stm->remove_persistent_state();
    }
    if (_archival_meta_stm) {
        co_await _archival_meta_stm->remove_persistent_state();
    }
    if (_id_allocator_stm) {
        co_await _id_allocator_stm->remove_persistent_state();
    }
}

ss::future<> partition::remove_remote_persistent_state(ss::abort_source& as) {
    // Backward compatibility: even if remote.delete is true, only do
    // deletion if the partition is in full tiered storage mode (this
    // excludes read replica clusters from deleting data in S3)
    bool tiered_storage = get_ntp_config().is_tiered_storage();

    if (
      _cloud_storage_partition && tiered_storage
      && get_ntp_config().remote_delete()) {
        vlog(
          clusterlog.debug,
          "Erasing S3 objects for partition {} ({} {} {})",
          ntp(),
          get_ntp_config(),
          get_ntp_config().is_archival_enabled(),
          get_ntp_config().is_read_replica_mode_enabled());
        co_await _cloud_storage_partition->erase(
          as, _raft->self(), group_configuration());
    } else if (_cloud_storage_partition && tiered_storage) {
        // Tiered storage is enabled, but deletion is disabled: ensure the
        // remote metadata is up to date before we drop the local partition.
        vlog(
          clusterlog.info,
          "Leaving tiered storage objects behind for partition {}",
          ntp());
        co_await _cloud_storage_partition->finalize(
          as, _raft->self(), group_configuration());
    }
}

uint64_t partition::upload_backlog_size() const {
    if (_archiver) {
        return _archiver->estimate_backlog_size();
    } else {
        return 0;
    }
}

void partition::set_topic_config(
  std::unique_ptr<cluster::topic_configuration> cfg) {
    _topic_cfg = std::move(cfg);
    if (_archiver) {
        _archiver->notify_topic_config();
    }
}

ss::future<std::error_code>
partition::transfer_leadership(std::optional<model::node_id> target) {
    vlog(
      clusterlog.debug,
      "Transferring {} leadership to {}",
      ntp(),
      target.value_or(model::node_id{-1}));

    // Some state machines need a preparatory phase to efficiently transfer
    // leadership: invoke this, and hold the lock that they return until
    // the leadership transfer attempt is complete.
    ss::basic_rwlock<>::holder stm_prepare_lock;
    if (_rm_stm) {
        stm_prepare_lock = co_await _rm_stm->prepare_transfer_leadership();
    } else if (_tm_stm) {
        stm_prepare_lock = co_await _tm_stm->prepare_transfer_leadership();
    }

    std::optional<ss::deferred_action<std::function<void()>>> complete_archiver;
    auto archival_timeout
      = config::shard_local_cfg().cloud_storage_graceful_transfer_timeout_ms();
    if (_archiver && archival_timeout.has_value()) {
        complete_archiver.emplace(
          [a = _archiver.get()]() { a->complete_transfer_leadership(); });
        vlog(
          clusterlog.debug,
          "transfer_leadership[{}]: entering archiver prepare",
          ntp());

        bool archiver_clean = co_await _archiver->prepare_transfer_leadership(
          archival_timeout.value());
        if (!archiver_clean) {
            // This is legal: if we are very tight on bandwidth to S3, then it
            // can take longer than the available timeout for an upload of
            // a large segment to complete.  If this happens, we will leak
            // an object, but retain a consistent+correct manifest when
            // the new leader writes it.
            vlog(
              clusterlog.warn,
              "Timed out waiting for {} uploads to complete before "
              "transferring leadership: proceeding anyway",
              ntp());
        } else {
            vlog(
              clusterlog.debug,
              "transfer_leadership[{}]: archiver prepare complete",
              ntp());
        }
    } else {
        vlog(clusterlog.trace, "transfer_leadership[{}]: no archiver", ntp());
    }

    co_return co_await _raft->do_transfer_leadership(target);
}

std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
} // namespace cluster
