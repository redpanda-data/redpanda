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
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote_partition.h"
#include "cluster/logger.h"
#include "cluster/tm_stm_cache_manager.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/state_machine_manager.h"
#include "raft/types.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <exception>

namespace {
bool is_id_allocator_topic(model::ntp ntp) {
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::id_allocator_topic;
}

bool is_tx_manager_topic(const model::ntp& ntp) {
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::tx_manager_topic;
}

bool is_transform_offsets_topic(const model::ntp& ntp) {
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::transform_offsets_topic;
}
}; // namespace
namespace cluster {

partition::partition(
  consensus_ptr r,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_storage_cache,
  ss::lw_shared_ptr<const archival::configuration> archival_conf,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::tm_stm_cache_manager>& tm_stm_cache_manager,
  ss::sharded<archival::upload_housekeeping_service>& upload_hks,
  ss::sharded<producer_state_manager>& producer_state_manager,
  storage::kvstore& kvstore,
  std::optional<cloud_storage_clients::bucket_name> read_replica_bucket)
  : _raft(std::move(r))
  , _partition_mem_tracker(
      ss::make_shared<util::mem_tracker>(_raft->ntp().path()))
  , _probe(std::make_unique<replicated_partition_probe>(*this))
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _feature_table(feature_table)
  , _tm_stm_cache_manager(tm_stm_cache_manager)
  , _is_tx_enabled(config::shard_local_cfg().enable_transactions.value())
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value())
  , _archival_conf(std::move(archival_conf))
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_storage_cache(cloud_storage_cache)
  , _cloud_storage_probe(
      ss::make_shared<cloud_storage::partition_probe>(_raft->ntp()))
  , _upload_housekeeping(upload_hks)
  , _kvstore(kvstore)
  , _producer_state_manager(producer_state_manager) {
    // Construct cloud_storage read path (remote_partition)
    if (
      config::shard_local_cfg().cloud_storage_enabled()
      && _cloud_storage_api.local_is_initialized()
      && _raft->ntp().ns == model::kafka_namespace) {
        if (_cloud_storage_cache.local_is_initialized()) {
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
        }
    }
}

partition::~partition() {}

ss::future<std::error_code> partition::prefix_truncate(
  model::offset rp_start_offset,
  kafka::offset kafka_start_offset,
  ss::lowres_clock::time_point deadline) {
    if (!_log_eviction_stm || !_raft->log_config().is_collectable()) {
        vlog(
          clusterlog.info,
          "Cannot prefix-truncate topic/partition {} retention settings not "
          "applied",
          _raft->ntp());
        co_return make_error_code(errc::topic_invalid_config);
    }
    if (!feature_table().local().is_active(features::feature::delete_records)) {
        vlog(
          clusterlog.info,
          "Cannot prefix-truncate topic/partition {} feature is currently "
          "disabled",
          _raft->ntp());
        co_return make_error_code(cluster::errc::feature_disabled);
    }
    vlog(
      clusterlog.info,
      "Truncating {} to redpanda offset {} kafka offset {}",
      _raft->ntp(),
      rp_start_offset,
      kafka_start_offset);
    auto res = co_await _log_eviction_stm->truncate(
      rp_start_offset, kafka_start_offset, deadline, _as);
    if (res.has_error()) {
        co_return res.error();
    }
    if (_archival_meta_stm) {
        // The archival metadata stm also listens for prefix_truncate batches.
        auto truncate_batch_offset = res.value();
        auto applied = co_await _archival_meta_stm->wait_no_throw(
          truncate_batch_offset, deadline, _as);
        if (applied) {
            co_return errc::success;
        }
        if (_as.abort_requested()) {
            co_return errc::shutting_down;
        }
        co_return errc::timeout;
    }
    co_return errc::success;
}

ss::future<std::vector<rm_stm::tx_range>> partition::aborted_transactions_cloud(
  const cloud_storage::offset_range& offsets) {
    return _cloud_storage_partition->aborted_transactions(offsets);
}

cluster::cloud_storage_mode partition::get_cloud_storage_mode() const {
    if (!config::shard_local_cfg().cloud_storage_enabled()) {
        return cluster::cloud_storage_mode::disabled;
    }

    const auto& cfg = _raft->log_config();

    if (cfg.is_read_replica_mode_enabled()) {
        return cluster::cloud_storage_mode::read_replica;
    }
    if (cfg.is_tiered_storage()) {
        return cluster::cloud_storage_mode::full;
    }
    if (cfg.is_archival_enabled()) {
        return cluster::cloud_storage_mode::write_only;
    }
    if (cfg.is_remote_fetch_enabled()) {
        return cluster::cloud_storage_mode::read_only;
    }

    return cluster::cloud_storage_mode::disabled;
}

partition_cloud_storage_status partition::get_cloud_storage_status() const {
    auto wrap_model_offset =
      [this](model::offset o) -> std::optional<kafka::offset> {
        if (o == model::offset{}) {
            return std::nullopt;
        }
        return model::offset_cast(
          get_offset_translator_state()->from_log_offset(o));
    };

    auto time_point_to_delta = [](ss::lowres_clock::time_point tp)
      -> std::optional<std::chrono::milliseconds> {
        if (tp.time_since_epoch().count() == 0) {
            return std::nullopt;
        }

        return std::chrono::duration_cast<std::chrono::milliseconds>(
          ss::lowres_clock::now() - tp);
    };

    partition_cloud_storage_status status;

    status.mode = get_cloud_storage_mode();

    const auto local_log = _raft->log();
    status.local_log_size_bytes = local_log->size_bytes();
    status.local_log_segment_count = local_log->segment_count();

    const auto local_log_offsets = local_log->offsets();
    status.local_log_start_offset = wrap_model_offset(
      local_log_offsets.start_offset);
    /**
     * If committed offset is smaller than the log start offset it indicates
     * that committed offset wasn't yet established (log is empty)
     */
    if (local_log_offsets.committed_offset >= local_log_offsets.start_offset) {
        status.local_log_last_offset = wrap_model_offset(
          local_log_offsets.committed_offset);
    }

    if (status.mode != cloud_storage_mode::disabled && _archival_meta_stm) {
        const auto& manifest = _archival_meta_stm->manifest();
        status.cloud_metadata_update_pending
          = _archival_meta_stm->get_dirty()
            == archival_metadata_stm::state_dirty::dirty;
        status.cloud_log_size_bytes = manifest.cloud_log_size();
        status.stm_region_size_bytes = manifest.stm_region_size_bytes();
        status.archive_size_bytes = manifest.archive_size_bytes();
        status.stm_region_segment_count = manifest.size();

        if (manifest.size() > 0) {
            status.cloud_log_start_offset
              = manifest.full_log_start_kafka_offset();
            status.stm_region_start_offset = manifest.get_start_kafka_offset();
            status.cloud_log_last_offset = manifest.get_last_kafka_offset();
        }

        // Calculate local space usage that does not overlap with cloud space
        const auto local_space_excl = status.cloud_log_last_offset
                                        ? _raft->log()->size_bytes_after_offset(
                                          manifest.get_last_offset())
                                        : status.local_log_size_bytes;

        status.total_log_size_bytes = status.cloud_log_size_bytes
                                      + local_space_excl;
    } else {
        status.total_log_size_bytes = status.local_log_size_bytes;
    }

    if (is_leader() && _archiver) {
        if (
          status.mode == cloud_storage_mode::write_only
          || status.mode == cloud_storage_mode::full) {
            status.since_last_manifest_upload = time_point_to_delta(
              _archiver->get_last_manfiest_upload_time());
            status.since_last_segment_upload = time_point_to_delta(
              _archiver->get_last_segment_upload_time());
        } else if (status.mode == cloud_storage_mode::read_replica) {
            const auto last_sync_at = _archiver->get_last_sync_time();
            if (last_sync_at) {
                status.since_last_manifest_sync = time_point_to_delta(
                  *last_sync_at);
            } else {
                status.since_last_manifest_sync = std::nullopt;
            }
        }
    }

    return status;
}

std::optional<cloud_storage::anomalies>
partition::get_cloud_storage_anomalies() const {
    if (!_archival_meta_stm || !is_leader()) {
        return std::nullopt;
    }

    return _archival_meta_stm->manifest().detected_anomalies();
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

std::optional<uint64_t> partition::cloud_log_size() const {
    if (_cloud_storage_partition == nullptr) {
        return std::nullopt;
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

model::offset partition::next_cloud_offset() const {
    vassert(
      cloud_data_available(),
      "Method can only be called if cloud data is available, ntp: {}",
      _raft->ntp());
    return kafka::offset_cast(_cloud_storage_partition->next_kafka_offset());
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
    co_return ret_t(
      kafka_result{kafka::offset(get_offset_translator_state()->from_log_offset(
        res.value().last_offset)())});
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

    if (bid.is_idempotent()) {
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
            get_offset_translator_state()->from_log_offset(old_offset)());
          return ret_t(kafka_result{new_offset});
      });
    return kafka_stages(
      std::move(res.request_enqueued), std::move(replicate_finished));
}

ss::future<> partition::start(std::optional<topic_configuration> topic_cfg) {
    const auto& ntp = _raft->ntp();
    raft::state_machine_manager_builder builder;
    // special cases for id_allocator and transaction coordinator partitions
    if (is_id_allocator_topic(ntp)) {
        _id_allocator_stm = builder.create_stm<cluster::id_allocator_stm>(
          clusterlog, _raft.get());
        co_return co_await _raft->start(std::move(builder));
    }

    if (!storage::deletion_exempt(_raft->ntp())) {
        _log_eviction_stm = builder.create_stm<cluster::log_eviction_stm>(
          _raft.get(), clusterlog, _kvstore);
        _raft->log()->stm_manager()->add_stm(_log_eviction_stm);
    }
    if (is_tx_manager_topic(_raft->ntp()) && _is_tx_enabled) {
        _tm_stm = builder.create_stm<cluster::tm_stm>(
          clusterlog,
          _raft.get(),
          _feature_table,
          _tm_stm_cache_manager.local().get(_raft->ntp().tp.partition));
        _raft->log()->stm_manager()->add_stm(_tm_stm);
        co_return co_await _raft->start(std::move(builder));
    }

    if (is_transform_offsets_topic(_raft->ntp())) {
        vassert(
          topic_cfg.has_value(),
          "No topic configuration passed, stm requires configuration for "
          "partition count.");
        _transform_offsets_stm = builder.create_stm<transform_offsets_stm_t>(
          topic_cfg->partition_count, clusterlog, _raft.get());
    }
    /**
     * Data partitions
     */
    const model::topic_namespace_view tp_ns(_raft->ntp());
    const bool is_group_ntp = tp_ns == model::kafka_consumer_offsets_nt;
    const bool has_rm_stm = (_is_tx_enabled || _is_idempotence_enabled)
                            && model::controller_ntp != _raft->ntp()
                            && !is_group_ntp;

    if (has_rm_stm) {
        _rm_stm = builder.create_stm<cluster::rm_stm>(
          clusterlog,
          _raft.get(),
          _tx_gateway_frontend,
          _feature_table,
          _producer_state_manager);
        _raft->log()->stm_manager()->add_stm(_rm_stm);
    }

    // Construct cloud_storage read path (remote_partition)
    if (
      config::shard_local_cfg().cloud_storage_enabled()
      && _cloud_storage_api.local_is_initialized()
      && _raft->ntp().ns == model::kafka_namespace) {
        _archival_meta_stm = builder.create_stm<cluster::archival_metadata_stm>(
          _raft.get(),
          _cloud_storage_api.local(),
          _feature_table.local(),
          clusterlog,
          _partition_mem_tracker);
        _raft->log()->stm_manager()->add_stm(_archival_meta_stm);

        if (_cloud_storage_cache.local_is_initialized()) {
            const auto& bucket_config
              = cloud_storage::configuration::get_bucket_config();
            auto bucket = bucket_config.value();
            if (
              _read_replica_bucket
              && _raft->log_config().is_read_replica_mode_enabled()) {
                vlog(
                  clusterlog.info,
                  "{} Remote topic bucket is {}",
                  _raft->ntp(),
                  _read_replica_bucket);
                // Override the bucket for read replicas
                bucket = _read_replica_bucket;
            }
            if (!bucket) {
                throw std::runtime_error{fmt::format(
                  "configuration property {} is not set",
                  bucket_config.name())};
            }

            _cloud_storage_manifest_view
              = ss::make_shared<cloud_storage::async_manifest_view>(
                _cloud_storage_api,
                _cloud_storage_cache,
                _archival_meta_stm->manifest(),
                cloud_storage_clients::bucket_name{*bucket});

            _cloud_storage_partition
              = ss::make_shared<cloud_storage::remote_partition>(
                _cloud_storage_manifest_view,
                _cloud_storage_api.local(),
                _cloud_storage_cache.local(),
                cloud_storage_clients::bucket_name{*bucket},
                *_cloud_storage_probe);
        }
    }

    // Start the probe after the partition is fully initialised, but before
    // starting everything.
    _probe.setup_metrics(ntp);

    if (_cloud_storage_manifest_view) {
        co_await _cloud_storage_manifest_view->start();
    }

    if (_cloud_storage_partition) {
        co_await _cloud_storage_partition->start();
    }

    {
        auto archiver_reset_guard = co_await ssx::with_timeout_abortable(
          ss::get_units(_archiver_reset_mutex, 1),
          ss::lowres_clock::now() + archiver_reset_mutex_timeout,
          _as);

        maybe_construct_archiver();

        if (_archiver) {
            co_await _archiver->start();
        }
    }

    co_return co_await _raft->start(std::move(builder));
}

ss::future<> partition::stop() {
    auto partition_ntp = ntp();
    vlog(clusterlog.debug, "Stopping partition: {}", partition_ntp);
    _as.request_abort();

    {
        // `partition_manager::do_shutdown` (caller of stop) will assert
        // out on any thrown exceptions. Hence, acquire the units without
        // a timeout or abort source.
        auto archiver_reset_guard = co_await ss::get_units(
          _archiver_reset_mutex, 1);

        if (_archiver) {
            _upload_housekeeping.local().deregister_jobs(
              _archiver->get_housekeeping_jobs());
            vlog(
              clusterlog.debug,
              "Stopping archiver on partition: {}",
              partition_ntp);
            co_await _archiver->stop();
        }
    }

    if (_cloud_storage_partition) {
        vlog(
          clusterlog.debug,
          "Stopping cloud_storage_partition on partition: {}",
          partition_ntp);
        co_await _cloud_storage_partition->stop();
    }

    if (_cloud_storage_manifest_view) {
        vlog(
          clusterlog.debug,
          "Stopping cloud_storage_manifest_view on partition: {}",
          partition_ntp);
        co_await _cloud_storage_manifest_view->stop();
    }

    _probe.clear_metrics();
    vlog(clusterlog.debug, "Stopped partition {}", partition_ntp);
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(storage::timequery_config cfg) {
    // Read replicas never consider local raft data
    if (_raft->log_config().is_read_replica_mode_enabled()) {
        co_return co_await cloud_storage_timequery(cfg);
    }

    const bool may_answer_from_cloud
      = may_read_from_cloud()
        && _cloud_storage_partition->bounds_timestamp(cfg.time)
        && cfg.min_offset < kafka::offset_cast(
             _cloud_storage_partition->next_kafka_offset());

    if (_raft->log()->start_timestamp() <= cfg.time) {
        // The query is ahead of the local data's start_timestamp: this
        // means it _might_ hit on local data: start_timestamp is not
        // precise, so once we query we might still fall back to cloud
        // storage
        //
        // We also need to adjust the lower bound for the local query as the
        // min_offset corresponds to the full log (including tiered storage).
        auto local_query_cfg = cfg;
        local_query_cfg.min_offset = std::max(
          _raft->get_offset_translator_state()->from_log_offset(
            _raft->start_offset()),
          local_query_cfg.min_offset);

        // If the min_offset is ahead of max_offset, the local log is empty
        // or was truncated since the timequery_config was created.
        if (local_query_cfg.min_offset > local_query_cfg.max_offset) {
            co_return std::nullopt;
        }

        auto result = co_await local_timequery(
          local_query_cfg, may_answer_from_cloud);
        if (result.has_value()) {
            co_return result;
        } else {
            // The local storage hit a case where it needs to fall back
            // to querying cloud storage.
            co_return co_await cloud_storage_timequery(cfg);
        }
    } else {
        if (may_answer_from_cloud) {
            // Timestamp is before local storage but within cloud storage
            co_return co_await cloud_storage_timequery(cfg);
        } else {
            // No cloud data OR not allowed to read from cloud: queries earlier
            // than the start of the log will hit on the start of the log.
            //
            // Adjust the lower bound for the local query as the min_offset
            // corresponds to the full log (including tiered storage).
            auto local_query_cfg = cfg;
            local_query_cfg.min_offset = std::max(
              _raft->get_offset_translator_state()->from_log_offset(
                _raft->start_offset()),
              local_query_cfg.min_offset);

            // If the min_offset is ahead of max_offset, the local log is empty
            // or was truncated since the timequery_config was created.
            if (local_query_cfg.min_offset > local_query_cfg.max_offset) {
                co_return std::nullopt;
            }

            co_return co_await local_timequery(local_query_cfg, false);
        }
    }
}

bool partition::may_read_from_cloud() const {
    return (is_remote_fetch_enabled() || is_read_replica_mode_enabled())
           && (_cloud_storage_partition && _cloud_storage_partition->is_data_available());
}

ss::future<std::optional<storage::timequery_result>>
partition::cloud_storage_timequery(storage::timequery_config cfg) {
    if (!may_read_from_cloud()) {
        co_return std::nullopt;
    }

    // We have data in the remote partition, and all the data in the
    // raft log is ahead of the query timestamp or the topic is a read
    // replica, so proceed to query the remote partition to try and
    // find the earliest data that has timestamp >= the query time.
    vlog(clusterlog.debug, "timequery (cloud) {} cfg(k)={}", _raft->ntp(), cfg);

    // remote_partition pre-translates offsets for us, so no call into
    // the offset translator here
    auto result = co_await _cloud_storage_partition->timequery(cfg);
    if (result.has_value()) {
        vlog(
          clusterlog.debug,
          "timequery (cloud) {} cfg(k)={} result(k)={}",
          _raft->ntp(),
          cfg,
          result->offset);
    }

    co_return result;
}

ss::future<std::optional<storage::timequery_result>> partition::local_timequery(
  storage::timequery_config cfg, bool allow_cloud_fallback) {
    vlog(clusterlog.debug, "timequery (raft) {} cfg(k)={}", _raft->ntp(), cfg);

    cfg.min_offset = _raft->get_offset_translator_state()->to_log_offset(
      cfg.min_offset);
    cfg.max_offset = _raft->get_offset_translator_state()->to_log_offset(
      cfg.max_offset);

    vlog(clusterlog.debug, "timequery (raft) {} cfg(r)={}", _raft->ntp(), cfg);

    auto result = co_await _raft->timequery(cfg);

    if (result.has_value()) {
        if (allow_cloud_fallback) {
            // We need to test for cases in which we will fall back to querying
            // cloud storage.
            if (_raft->log()->start_timestamp() > cfg.time) {
                // Query raced with prefix truncation
                vlog(
                  clusterlog.debug,
                  "timequery (raft) {} cfg(r)={} raced with truncation "
                  "(start_timestamp {}, result {})",
                  _raft->ntp(),
                  cfg,
                  _raft->log()->start_timestamp(),
                  result->time);
                co_return std::nullopt;
            }

            if (
              _raft->log()->start_timestamp() <= cfg.time
              && result->time > cfg.time) {
                // start_timestamp() points to the beginning of the oldest
                // segment, but start_offset points to somewhere within a
                // segment.  If our timequery hits the range between the start
                // of segment and the start_offset, consensus::timequery may
                // answer with the start offset rather than the
                // pre-start-offset location where the timestamp is actually
                // found. Ref
                // https://github.com/redpanda-data/redpanda/issues/9669
                vlog(
                  clusterlog.debug,
                  "Timequery (raft) {} cfg(r)={} miss on local log "
                  "(start_timestamp "
                  "{}, result {})",
                  _raft->ntp(),
                  cfg,
                  _raft->log()->start_timestamp(),
                  result->time);
                co_return std::nullopt;
            }
        }

        if (result->offset == _raft->log()->offsets().start_offset) {
            // If we hit at the start of the local log, this is ambiguous:
            // there could be earlier batches prior to start_offset which
            // have the same timestamp and are present in cloud storage.
            vlog(
              clusterlog.debug,
              "Timequery (raft) {} cfg(r)={} hit start_offset in local log "
              "(start_offset {} start_timestamp {}, result {})",
              _raft->ntp(),
              cfg,
              _raft->log()->offsets().start_offset,
              _raft->log()->start_timestamp(),
              cfg.time);

            if (allow_cloud_fallback) {
                // Even though we hit data with the desired timestamp, we
                // cannot be certain that this is the _first_ batch with
                // the desired timestamp: return null so that the caller
                // will fall back to cloud storage.
                co_return std::nullopt;
            }
        }

        vlog(
          clusterlog.debug,
          "timequery (raft) {} cfg(r)={} result(r)={}",
          _raft->ntp(),
          cfg,
          result->offset);
        result->offset = _raft->get_offset_translator_state()->from_log_offset(
          result->offset);
    }

    co_return result;
}

bool partition::should_construct_archiver() {
    // NOTE: construct and archiver even if shadow indexing isn't enabled, e.g.
    // in the case of read replicas -- we still need the archiver to drive
    // manifest updates, etc.
    const auto& ntp_config = _raft->log()->config();
    return config::shard_local_cfg().cloud_storage_enabled()
           && _cloud_storage_api.local_is_initialized()
           && _raft->ntp().ns == model::kafka_namespace
           && (ntp_config.is_archival_enabled() || ntp_config.is_read_replica_mode_enabled());
}

void partition::maybe_construct_archiver() {
    if (should_construct_archiver()) {
        const auto& ntp_config = _raft->log()->config();
        _archiver = std::make_unique<archival::ntp_archiver>(
          ntp_config,
          _archival_conf,
          _cloud_storage_api.local(),
          _cloud_storage_cache.local(),
          *this,
          _cloud_storage_manifest_view);
        if (!ntp_config.is_read_replica_mode_enabled()) {
            _upload_housekeeping.local().register_jobs(
              _archiver->get_housekeeping_jobs());
        }
    }
}

uint64_t partition::non_log_disk_size_bytes() const {
    uint64_t raft_size = _raft->get_snapshot_size();

    std::optional<uint64_t> rm_size;
    if (_rm_stm) {
        rm_size = _rm_stm->get_local_snapshot_size();
    }

    std::optional<uint64_t> tm_size;
    if (_tm_stm) {
        tm_size = _tm_stm->get_local_snapshot_size();
    }

    std::optional<uint64_t> archival_size;
    if (_archival_meta_stm) {
        archival_size = _archival_meta_stm->get_local_snapshot_size();
    }

    std::optional<uint64_t> idalloc_size;
    if (_id_allocator_stm) {
        idalloc_size = _id_allocator_stm->get_local_snapshot_size();
    }

    vlog(
      clusterlog.trace,
      "non-log disk size: raft {} rm {} tm {} archival {} idalloc {}",
      raft_size,
      rm_size,
      tm_size,
      archival_size,
      idalloc_size);

    return raft_size + rm_size.value_or(0) + tm_size.value_or(0)
           + archival_size.value_or(0) + idalloc_size.value_or(0);
}

ss::future<> partition::update_configuration(topic_properties properties) {
    auto& old_ntp_config = _raft->log()->config();
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
    co_await _raft->log()->update_configuration(new_ntp_config);

    // Update cached instance of topic properties
    if (_topic_cfg) {
        _topic_cfg->properties = std::move(properties);
    }

    // If this partition's cloud storage mode changed, rebuild the archiver.
    // This must happen after raft update, because it reads raft's
    // ntp_config to decide whether to construct an archiver.
    if (cloud_storage_changed) {
        vlog(
          clusterlog.debug,
          "update_configuration[{}]: updating archiver for config {}",
          new_ntp_config,
          _raft->ntp());

        auto archiver_reset_guard = co_await ssx::with_timeout_abortable(
          ss::get_units(_archiver_reset_mutex, 1),
          ss::lowres_clock::now() + archiver_reset_mutex_timeout,
          _as);

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
    auto o = _raft->log()->get_term_last_offset(term);
    if (!o) {
        return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next
    // leader epoch
    return model::next_offset(*o);
}

ss::future<std::optional<model::offset>>
partition::get_cloud_term_last_offset(model::term_id term) const {
    auto o = co_await _cloud_storage_partition->get_term_last_offset(term);
    if (!o) {
        co_return std::nullopt;
    }
    // Kafka defines leader epoch last offset as a first offset of next
    // leader epoch
    co_return model::next_offset(kafka::offset_cast(*o));
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
    if (_log_eviction_stm) {
        co_await _log_eviction_stm->remove_persistent_state();
    }
}

/**
 * Return the index of this node in the list of voters, or nullopt if it
 * is not a voter.
 */
static std::optional<size_t>
voter_position(raft::vnode self, const raft::group_configuration& raft_config) {
    const auto& voters = raft_config.current_config().voters;
    auto position = std::find(voters.begin(), voters.end(), self);
    if (position == voters.end()) {
        return std::nullopt;
    } else {
        return position - voters.begin();
    }
}

// To reduce redundant re-uploads in the typical case of all replicas
// being alive, have all non-0th replicas delay before attempting to
// reconcile the manifest. This is just a best-effort thing, it is
// still okay for them to step on each other: finalization is best
// effort and the worst case outcome is to leave behind a few orphan
// objects if writes were ongoing while deletion happened.
static ss::future<bool> should_finalize(
  ss::abort_source& as,
  raft::vnode self,
  const raft::group_configuration& raft_config) {
    static constexpr ss::lowres_clock::duration erase_non_0th_delay = 200ms;

    auto my_position = voter_position(self, raft_config);
    if (my_position.has_value()) {
        auto p = my_position.value();
        if (p != 0) {
            co_await ss::sleep_abortable(erase_non_0th_delay * p, as);
        }
        co_return true;
    } else {
        co_return false;
    }
}

ss::future<> partition::finalize_remote_partition(ss::abort_source& as) {
    if (!_feature_table.local().is_active(
          features::feature::cloud_storage_manifest_format_v2)) {
        // this is meant to prevent uploading manifests with new format
        // while the cluster is in a mixed state
        vlog(
          clusterlog.info, "skipping finalize of remote partition {}", ntp());
        co_return;
    }

    const bool tiered_storage = get_ntp_config().is_archival_enabled();

    if (_cloud_storage_partition && tiered_storage) {
        const auto finalize = co_await should_finalize(
          as, _raft->self(), group_configuration());

        if (finalize) {
            vlog(
              clusterlog.debug,
              "Finalizing remote metadata on partition delete {}",
              ntp());
            _cloud_storage_partition->finalize();
        }
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

ss::future<> partition::serialize_json_manifest_to_output_stream(
  ss::output_stream<char>& output) {
    if (!_archival_meta_stm || !_cloud_storage_partition) {
        throw std::runtime_error(fmt::format(
          "{} not configured for cloud storage", _topic_cfg->tp_ns));
    }

    // The timeout here is meant to place an upper bound on the amount
    // of time the manifest lock is held for.
    co_await ss::with_timeout(
      model::timeout_clock::now() + manifest_serialization_timeout,
      _cloud_storage_partition->serialize_json_manifest_to_output_stream(
        output));
}

ss::future<std::error_code>
partition::transfer_leadership(transfer_leadership_request req) {
    auto target = req.target;

    vlog(
      clusterlog.debug,
      "Transferring {} leadership to {}",
      ntp(),
      target.value_or(model::node_id{-1}));

    std::optional<ss::deferred_action<std::function<void()>>> complete_archiver;
    auto archival_timeout
      = config::shard_local_cfg().cloud_storage_graceful_transfer_timeout_ms();
    if (_archiver && archival_timeout.has_value()) {
        complete_archiver.emplace([this]() {
            if (_archiver) {
                _archiver->complete_transfer_leadership();
            }
        });

        vlog(
          clusterlog.debug,
          "transfer_leadership[{}]: entering archiver prepare",
          ntp());

        bool archiver_clean = co_await _archiver->prepare_transfer_leadership(
          archival_timeout.value());
        if (!archiver_clean) {
            // This is legal: if we are very tight on bandwidth to S3,
            // then it can take longer than the available timeout for an
            // upload of a large segment to complete.  If this happens, we
            // will leak an object, but retain a consistent+correct
            // manifest when the new leader writes it.
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

    // Some state machines need a preparatory phase to efficiently transfer
    // leadership: invoke this, and hold the lock that they return until
    // the leadership transfer attempt is complete.
    ss::basic_rwlock<>::holder stm_prepare_lock;
    if (_rm_stm) {
        stm_prepare_lock = co_await _rm_stm->prepare_transfer_leadership();
    } else if (_tm_stm) {
        stm_prepare_lock = co_await _tm_stm->prepare_transfer_leadership();
    }

    co_return co_await _raft->do_transfer_leadership(req);
}

result<std::vector<raft::follower_metrics>>
partition::get_follower_metrics() const {
    if (!_raft->is_leader()) {
        return errc::not_leader;
    };
    return _raft->get_follower_metrics();
}

ss::future<>
partition::replicate_unsafe_reset(cloud_storage::partition_manifest manifest) {
    vlog(
      clusterlog.info,
      "Replicating replace manifest command. New manifest details: {{ "
      "start_offset: {}, last_offset: {}}}",
      manifest.get_start_offset(),
      manifest.get_last_offset());

    // Replicate the reset command which contains the downloaded manifest
    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto replication_deadline = ss::lowres_clock::now() + sync_timeout;
    std::vector<cluster::command_batch_builder> builders;

    auto reset_builder = _archival_meta_stm->batch_start(
      replication_deadline, _as);
    reset_builder.replace_manifest(manifest.to_iobuf());

    auto errc = co_await reset_builder.replicate();
    if (errc) {
        if (errc == raft::errc::shutting_down) {
            // During shutdown, act like we hit an abort source rather
            // than trying to log+handle this like a write error.
            throw ss::abort_requested_exception();
        }

        vlog(
          clusterlog.warn,
          "[{}] Unsafe reset failed to update archival STM: "
          "{}",
          ntp(),
          errc.message());
        throw std::runtime_error(
          fmt::format("Failed to update archival STM: {}", errc.message()));
    }

    vlog(
      clusterlog.info,
      "[{}] Unsafe reset replicated STM commands successfully",
      ntp());
}

ss::future<>
partition::unsafe_reset_remote_partition_manifest_from_json(iobuf json_buf) {
    vlog(clusterlog.info, "[{}] Manual unsafe manifest reset requested", ntp());

    if (!(config::shard_local_cfg().cloud_storage_enabled()
          && _archival_meta_stm)) {
        vlog(
          clusterlog.warn,
          "[{}] Archival STM not present. Skipping unsafe reset ...",
          ntp());
        throw std::runtime_error("Archival STM not present");
    }

    if (_archiver != nullptr) {
        vlog(
          clusterlog.warn,
          "[{}] Remote write path in use. Skipping unsafe reset ...",
          ntp());
        throw std::runtime_error("Remote write path in use");
    }

    // Deserialise provided manifest
    cloud_storage::partition_manifest req_m{
      _raft->ntp(), _raft->log_config().get_initial_revision()};
    req_m.update_with_json(std::move(json_buf));

    co_await replicate_unsafe_reset(std::move(req_m));
}

ss::future<>
partition::unsafe_reset_remote_partition_manifest_from_cloud(bool force) {
    vlog(
      clusterlog.info,
      "[{}] Unsafe manifest reset from cloud state requested",
      ntp());

    _as.check();

    if (!(config::shard_local_cfg().cloud_storage_enabled()
          && _archival_meta_stm)) {
        vlog(
          clusterlog.warn,
          "[{}] Archival STM not present. Skipping unsafe reset ...",
          ntp());
        throw std::runtime_error("Archival STM not present");
    }

    std::optional<ssx::semaphore_units> archiver_reset_guard;
    if (should_construct_archiver()) {
        archiver_reset_guard = co_await ssx::with_timeout_abortable(
          ss::get_units(_archiver_reset_mutex, 1),
          ss::lowres_clock::now() + archiver_reset_mutex_timeout,
          _as);
    }

    // Stop the archiver and its housekeeping jobs
    if (_archiver) {
        _upload_housekeeping.local().deregister_jobs(
          _archiver->get_housekeeping_jobs());
        co_await _archiver->stop();
        _archiver = nullptr;
    }

    auto start_archiver = [this]() {
        maybe_construct_archiver();
        if (_archiver) {
            // Topic configs may have changed while the archiver was
            // stopped, so mark them as dirty just in case.
            _archiver->notify_topic_config();
            return _archiver->start();
        }

        return ss::now();
    };

    // Ensure that all commands on the log have been applied.
    // No new archival commands should be replicated now.
    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto sync_result = co_await ss::coroutine::as_future(
      _archival_meta_stm->sync(sync_timeout));
    if (sync_result.failed() || sync_result.get() == false) {
        vlog(
          clusterlog.warn,
          "[{}] Could not sync with log. Skipping unsafe reset ...",
          ntp());

        co_await start_archiver();
        throw std::runtime_error("Could not sync with log");
    }

    _as.check();

    // Attempt the reset
    auto future_result = co_await ss::coroutine::as_future(
      do_unsafe_reset_remote_partition_manifest_from_cloud(force));

    // Reconstruct the archiver and start it if needed
    co_await start_archiver();

    // Rethrow the exception if we failed to reset
    future_result.get();
}

ss::future<>
partition::do_unsafe_reset_remote_partition_manifest_from_cloud(bool force) {
    const auto initial_rev = _raft->log_config().get_initial_revision();
    const auto bucket = [this]() {
        if (is_read_replica_mode_enabled()) {
            return get_read_replica_bucket();
        }

        const auto& bucket_config
          = cloud_storage::configuration::get_bucket_config();
        vassert(
          bucket_config.value(),
          "configuration property {} must be set",
          bucket_config.name());

        return cloud_storage_clients::bucket_name{
          bucket_config.value().value()};
    }();

    // Download the current partition manifest from the cloud
    cloud_storage::partition_manifest new_manifest{ntp(), initial_rev};

    auto timeout
      = config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms();
    auto backoff = config::shard_local_cfg().cloud_storage_initial_backoff_ms();

    retry_chain_node rtc(_as, timeout, backoff);
    auto [res, res_fmt]
      = co_await _cloud_storage_api.local().try_download_partition_manifest(
        bucket, new_manifest, rtc);

    if (res != cloud_storage::download_result::success) {
        throw std::runtime_error(ssx::sformat(
          "Failed to download partition manifest with error: {}", res));
    }

    const auto max_collectible
      = _raft->log()->stm_manager()->max_collectible_offset();
    if (new_manifest.get_last_offset() < max_collectible) {
        auto msg = ssx::sformat(
          "Applying the cloud manifest would cause data loss since the last "
          "offset in the downloaded manifest is below the max_collectible "
          "offset "
          "{} < {}",
          new_manifest.get_last_offset(),
          max_collectible);

        if (!force) {
            throw std::runtime_error(msg);
        }

        vlog(
          clusterlog.warn,
          "[{}] {}. Proceeding since the force flag was used.",
          ntp(),
          msg);
    }

    co_await replicate_unsafe_reset(std::move(new_manifest));
}

std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
} // namespace cluster
