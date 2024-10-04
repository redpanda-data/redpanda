// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"

#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote_partition.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/upload_housekeeping_service.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/log_eviction_stm.h"
#include "cluster/logger.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/rm_stm.h"
#include "cluster/tm_stm.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "raft/state_machine_manager.h"
#include "storage/ntp_config.h"
#include "utils/rwlock.h"

#include <seastar/core/shared_ptr_incomplete.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <chrono>

namespace cluster {

partition::partition(
  consensus_ptr r,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_storage_cache,
  ss::lw_shared_ptr<const archival::configuration> archival_conf,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<archival::upload_housekeeping_service>& upload_hks,
  std::optional<cloud_storage_clients::bucket_name> read_replica_bucket)
  : _raft(std::move(r))
  , _probe(std::make_unique<replicated_partition_probe>(*this))
  , _feature_table(feature_table)
  , _archival_conf(std::move(archival_conf))
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_storage_cache(cloud_storage_cache)
  , _cloud_storage_probe(
      ss::make_shared<cloud_storage::partition_probe>(_raft->ntp()))
  , _upload_housekeeping(upload_hks)
  , _log_cleanup_policy(config::shard_local_cfg().log_cleanup_policy.bind()) {
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

    _log_cleanup_policy.watch([this]() {
        if (_as.abort_requested()) {
            return ss::now();
        }
        auto changed = _raft->log()->notify_compaction_update();
        if (changed) {
            vlog(
              clusterlog.debug,
              "[{}] updating archiver for cluster config change in "
              "log_cleanup_policy",
              _raft->ntp());

            return restart_archiver(false);
        }
        return ss::now();
    });
}

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

ss::future<std::vector<tx::tx_range>> partition::aborted_transactions_cloud(
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
        return model::offset_cast(log()->from_log_offset(o));
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
    auto reader = std::move(r);

    auto maybe_units = co_await hold_writes_enabled();
    if (!maybe_units) {
        co_return ret_t(maybe_units.error());
    }
    auto res = co_await _raft->replicate(std::move(reader), opts);
    if (!res) {
        co_return ret_t(res.error());
    }
    co_return ret_t(kafka_result{
      kafka::offset(log()->from_log_offset(res.value().last_offset)())});
}

ss::shared_ptr<cluster::rm_stm> partition::rm_stm() {
    if (!_rm_stm) {
        vlog(
          clusterlog.error,
          "Topic {} doesn't support idempotent and transactional "
          "processing.",
          _raft->ntp());
    }
    return _rm_stm;
}

namespace {
template<class Units, class StagesFutureFunc>
ss::future<result<kafka_result>> stages_with_units_helper(
  ss::future<result<Units>> maybe_units_f,
  ss::promise<> enqueued_promise,
  StagesFutureFunc stages_future_func) {
    auto maybe_units = co_await std::move(maybe_units_f);
    if (!maybe_units.has_value()) {
        enqueued_promise.set_value();
        co_return maybe_units.error();
    }
    kafka_stages orig_stages = stages_future_func();
    co_await std::move(orig_stages.request_enqueued);
    enqueued_promise.set_value();
    co_return co_await std::move(orig_stages.replicate_finished);
}

template<class Units, class StagesFutureFunc>
kafka_stages stages_with_units(
  ss::future<result<Units>> maybe_units_f,
  StagesFutureFunc stages_future_func) {
    ss::promise<> enqueued_promise;
    auto enqueued_f = enqueued_promise.get_future();
    auto replicated_f = stages_with_units_helper(
      std::move(maybe_units_f),
      std::move(enqueued_promise),
      std::move(stages_future_func));
    return {std::move(enqueued_f), std::move(replicated_f)};
}
} // namespace

kafka_stages partition::replicate_in_stages(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    using ret_t = result<kafka_result>;

    if (bid.is_transactional) {
        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support transactional processing.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    if (bid.is_idempotent()) {
        if (!_rm_stm) {
            vlog(
              clusterlog.error,
              "Topic {} doesn't support idempotent requests.",
              _raft->ntp());
            return kafka_stages(raft::errc::timeout);
        }
    }

    return stages_with_units(
      hold_writes_enabled(),
      [this,
       bid = std::move(bid),
       r = std::move(r),
       opts = std::move(opts)]() mutable {
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
                  log()->from_log_offset(old_offset)());
                return ret_t(kafka_result{new_offset});
            });
          return kafka_stages(
            std::move(res.request_enqueued), std::move(replicate_finished));
      });
}

raft::group_id partition::group() const { return _raft->group(); }

ss::future<> partition::start(
  state_machine_registry& stm_registry,
  const std::optional<xshard_transfer_state>& xst_state) {
    const auto& ntp = _raft->ntp();
    raft::state_machine_manager_builder builder = stm_registry.make_builder_for(
      _raft.get());

    std::optional<raft::xshard_transfer_state> raft_xst_state;
    if (xst_state) {
        raft_xst_state = xst_state->raft;
    }
    co_await _raft->start(std::move(builder), std::move(raft_xst_state));
    // store rm_stm pointer in partition as this is commonly used stm
    _rm_stm = _raft->stm_manager()->get<cluster::rm_stm>();
    _log_eviction_stm = _raft->stm_manager()->get<cluster::log_eviction_stm>();
    // store _archival_meta_stm pointer in partition as this is commonly used
    // stm (in future we may decide to remove it from partition)
    _archival_meta_stm
      = _raft->stm_manager()->get<cluster::archival_metadata_stm>();

    // store partition properties stm offset for fast access
    _partition_properties_stm
      = _raft->stm_manager()->get<cluster::partition_properties_stm>();
    // Start the probe after the partition is fully initialised
    _probe.setup_metrics(ntp);

    // Construct cloud_storage read path (remote_partition)

    if (_archival_meta_stm && _cloud_storage_cache.local_is_initialized()) {
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
              "configuration property {} is not set", bucket_config.name())};
        }

        _cloud_storage_manifest_view
          = ss::make_shared<cloud_storage::async_manifest_view>(
            _cloud_storage_api,
            _cloud_storage_cache,
            _archival_meta_stm->manifest(),
            cloud_storage_clients::bucket_name{*bucket},
            _archival_meta_stm->path_provider());

        _cloud_storage_partition
          = ss::make_shared<cloud_storage::remote_partition>(
            _cloud_storage_manifest_view,
            _cloud_storage_api.local(),
            _cloud_storage_cache.local(),
            cloud_storage_clients::bucket_name{*bucket},
            *_cloud_storage_probe);
    }
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
          log()->from_log_offset(_raft->start_offset()),
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
              log()->from_log_offset(_raft->start_offset()),
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

    cfg.min_offset = _raft->log()->to_log_offset(cfg.min_offset);
    cfg.max_offset = _raft->log()->to_log_offset(cfg.max_offset);

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
        result->offset = _raft->log()->from_log_offset(result->offset);
    }

    co_return result;
}

bool partition::should_construct_archiver() {
    // NOTE: construct an archiver even if shadow indexing isn't enabled, e.g.
    // in the case of read replicas -- we still need the archiver to drive
    // manifest updates, etc.
    const auto& ntp_config = _raft->log()->config();
    return config::shard_local_cfg().cloud_storage_enabled()
           && config::shard_local_cfg().cloud_storage_disable_archiver_manager()
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
    uint64_t stm_local_size = 0;
    _raft->stm_manager()->for_each_stm(
      [this, &stm_local_size](
        const ss::sstring& name, const raft::state_machine_base& stm) {
          const auto sz = stm.get_local_state_size();
          vlog(
            clusterlog.trace,
            "local non-log disk size of {} stm {} = {} bytes",
            _raft->ntp(),
            name,
            sz);
          stm_local_size += sz;
      });

    vlog(
      clusterlog.trace,
      "local non-log disk size of {}: {}",
      _raft->ntp(),
      raft_size + stm_local_size);

    return raft_size + stm_local_size;
}

ss::future<> partition::update_configuration(topic_properties properties) {
    auto& old_ntp_config = _raft->log()->config();
    auto new_ntp_config = properties.get_ntp_cfg_overrides();

    // Before applying change, consider whether it changes cloud storage
    // mode
    bool cloud_storage_changed = false;

    bool old_archival = old_ntp_config.is_archival_enabled();
    bool new_archival = new_ntp_config.shadow_indexing_mode
                        && model::is_archival_enabled(
                          new_ntp_config.shadow_indexing_mode.value());

    auto old_retention_ms = old_ntp_config.has_overrides()
                              ? old_ntp_config.get_overrides().retention_time
                              : tristate<std::chrono::milliseconds>(
                                  std::nullopt);
    auto new_retention_ms = new_ntp_config.retention_time;

    auto old_retention_bytes
      = old_ntp_config.has_overrides()
          ? old_ntp_config.get_overrides().retention_bytes
          : tristate<size_t>(std::nullopt);
    auto new_retention_bytes = new_ntp_config.retention_bytes;

    if (old_archival != new_archival) {
        vlog(
          clusterlog.debug,
          "[{}] updating archiver for topic config change in "
          "archival_enabled",
          _raft->ntp());
        cloud_storage_changed = true;
    }
    if (old_retention_ms != new_retention_ms) {
        vlog(
          clusterlog.debug,
          "[{}] updating archiver for topic config change in "
          "retention_ms",
          _raft->ntp());
        cloud_storage_changed = true;
    }
    if (old_retention_bytes != new_retention_bytes) {
        vlog(
          clusterlog.debug,
          "[{}] updating archiver for topic config change in "
          "retention_bytes",
          _raft->ntp());
        cloud_storage_changed = true;
    }

    // Pass the configuration update into the storage layer
    _raft->log()->set_overrides(new_ntp_config);
    bool compaction_changed = _raft->log()->notify_compaction_update();
    if (compaction_changed) {
        vlog(
          clusterlog.debug,
          "[{}] updating archiver for topic config change in compaction",
          _raft->ntp());
        cloud_storage_changed = true;
    }

    // Update cached instance of topic properties
    if (_topic_cfg) {
        _topic_cfg->properties = std::move(properties);
    }

    // Pass the configuration update to the raft layer
    _raft->notify_config_update();

    // If this partition's cloud storage mode changed, rebuild the archiver.
    // This must happen after the raft+storage update, because it reads raft's
    // ntp_config to decide whether to construct an archiver.
    if (cloud_storage_changed) {
        co_await restart_archiver(true);
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

ss::future<> partition::restart_archiver(bool should_notify_topic_config) {
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
        if (should_notify_topic_config) {
            _archiver->notify_topic_config();
        }
        co_await _archiver->start();
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
    _cloud_storage_probe->clear_metrics();
    co_await _raft->stm_manager()->remove_local_state();
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
partition::transfer_leadership(raft::transfer_leadership_request req) {
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
    } else if (auto stm = tm_stm(); stm) {
        stm_prepare_lock = co_await stm->prepare_transfer_leadership();
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

    // TODO: move this logic to the ntp_archiver
    // currently, the 'batch_start' method will work even if there is an
    // active input queue instance. The batch replicated here may interrupt
    // some operation in the archiver but the correctness will not be
    // compromised.
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
    if (sync_result.failed() || sync_result.get() == std::nullopt) {
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
    cloud_storage::partition_manifest_downloader dl(
      bucket,
      _archival_meta_stm->path_provider(),
      ntp(),
      initial_rev,
      _cloud_storage_api.local());
    auto res = co_await dl.download_manifest(rtc, &new_manifest);
    if (res.has_error()) {
        throw std::runtime_error(ssx::sformat(
          "Failed to download partition manifest with error: {}", res.error()));
    }
    if (
      res.value()
      == cloud_storage::find_partition_manifest_outcome::no_matching_manifest) {
        throw std::runtime_error(ssx::sformat(
          "No matching manifest for {} rev {}", ntp(), initial_rev));
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

ss::shared_ptr<cloud_storage::async_manifest_view>
partition::get_cloud_storage_manifest_view() {
    return _cloud_storage_manifest_view;
}

ss::future<result<model::offset, std::error_code>>
partition::sync_kafka_start_offset_override(
  model::timeout_clock::duration timeout) {
    if (is_read_replica_mode_enabled()) {
        auto term = _raft->term();
        if (!co_await _archival_meta_stm->sync(timeout)) {
            if (term != _raft->term()) {
                co_return errc::not_leader;
            } else {
                co_return errc::timeout;
            }
        }
        auto start_kafka_offset
          = _archival_meta_stm->manifest().get_start_kafka_offset_override();

        co_return kafka::offset_cast(start_kafka_offset);
    }

    if (_log_eviction_stm) {
        auto offset_res = co_await _log_eviction_stm
                            ->sync_kafka_start_offset_override(timeout);
        if (offset_res.has_failure()) {
            co_return offset_res.as_failure();
        }
        if (offset_res.value() != kafka::offset{}) {
            co_return kafka::offset_cast(offset_res.value());
        }
    }

    if (!_archival_meta_stm) {
        co_return model::offset{};
    }

    // There are a few cases in which the log_eviction_stm will return a kafka
    // offset of `kafka::offset{}` for the start offset override.
    // - The topic was remotely recovered.
    // - A start offset override was never set.
    // - The broker has restarted and the log_eviction_stm couldn't recover the
    //   kafka offset for the start offset override.
    //
    // In all cases we'll need to fall back to the archival stm to figure out if
    // a start offset override exists, and if so, what it is.
    //
    // For this we'll sync the archival stm a single time to ensure we have the
    // most up-to-date manifest. From that point onwards the offset
    // `_archival_meta_stm->manifest().get_start_kafka_offset_override()` will
    // be correct without having to sync again. This is since the offset will
    // not change until another offset override has been applied to the log
    // eviction stm. And at that point the log eviction stm will be able to give
    // us the correct offset override.
    if (!_has_synced_archival_for_start_override) [[unlikely]] {
        auto term = _raft->term();
        if (!co_await _archival_meta_stm->sync(timeout)) {
            if (term != _raft->term()) {
                co_return errc::not_leader;
            } else {
                co_return errc::timeout;
            }
        }
        _has_synced_archival_for_start_override = true;
    }

    auto start_kafka_offset
      = _archival_meta_stm->manifest().get_start_kafka_offset_override();
    co_return kafka::offset_cast(start_kafka_offset);
}

model::offset partition::last_stable_offset() const {
    if (_rm_stm) {
        return _rm_stm->last_stable_offset();
    }

    return high_watermark();
}

std::optional<model::offset> partition::eviction_requested_offset() {
    if (_log_eviction_stm) {
        return _log_eviction_stm->eviction_requested_offset();
    } else {
        return std::nullopt;
    }
}

ss::shared_ptr<cluster::id_allocator_stm> partition::id_allocator_stm() const {
    return _raft->stm_manager()->get<cluster::id_allocator_stm>();
}

std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
ss::shared_ptr<cluster::tm_stm> partition::tm_stm() {
    return _raft->stm_manager()->get<cluster::tm_stm>();
}

ss::future<fragmented_vector<tx::tx_range>>
partition::aborted_transactions(model::offset from, model::offset to) {
    if (!_rm_stm) {
        return ss::make_ready_future<fragmented_vector<tx::tx_range>>(
          fragmented_vector<tx::tx_range>());
    }
    return _rm_stm->aborted_transactions(from, to);
}

model::producer_id partition::highest_producer_id() {
    auto pid = _rm_stm ? _rm_stm->highest_producer_id() : model::producer_id{};
    if (_archival_meta_stm) {
        // It's possible this partition is a recovery partition, in which
        // case the archival metadata may be higher than what's in the
        // rm_stm.
        return std::max(
          pid, _archival_meta_stm->manifest().highest_producer_id());
    }
    return pid;
}

const ss::shared_ptr<cluster::archival_metadata_stm>&
partition::archival_meta_stm() const {
    return _archival_meta_stm;
}

std::optional<model::offset> partition::kafka_start_offset_override() const {
    if (_log_eviction_stm && !is_read_replica_mode_enabled()) {
        auto o = _log_eviction_stm->kafka_start_offset_override();
        if (o != kafka::offset{}) {
            return kafka::offset_cast(o);
        }
    }
    if (_archival_meta_stm) {
        auto o
          = _archival_meta_stm->manifest().get_start_kafka_offset_override();
        if (o != kafka::offset{}) {
            return kafka::offset_cast(o);
        }
    }
    return std::nullopt;
}

std::optional<std::reference_wrapper<cluster::topic_configuration>>
partition::get_topic_config() {
    if (_topic_cfg) {
        return std::ref(*_topic_cfg);
    } else {
        return std::nullopt;
    }
}

ss::sharded<features::feature_table>& partition::feature_table() const {
    return _feature_table;
}

ss::shared_ptr<const cloud_storage::remote_partition>
partition::remote_partition() const {
    return _cloud_storage_partition;
}

ss::future<model::record_batch_reader> partition::make_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> debounce_deadline) {
    return _raft->make_reader(std::move(config), debounce_deadline);
}

model::term_id partition::term() const { return _raft->term(); }

bool partition::is_read_replica_mode_enabled() const {
    const auto& cfg = _raft->log_config();
    return cfg.is_read_replica_mode_enabled();
}

model::offset partition::raft_start_offset() const {
    return _raft->start_offset();
}

model::offset partition::committed_offset() const {
    return _raft->committed_offset();
}
model::offset partition::high_watermark() const {
    return model::next_offset(_raft->last_visible_index());
}
model::offset partition::leader_high_watermark() const {
    return model::next_offset(_raft->last_leader_visible_index());
}
model::offset partition::dirty_offset() const {
    return _raft->log()->offsets().dirty_offset;
}

const model::ntp& partition::ntp() const { return _raft->ntp(); }

ss::shared_ptr<storage::log> partition::log() const { return _raft->log(); }

bool partition::is_elected_leader() const { return _raft->is_elected_leader(); }
bool partition::is_leader() const { return _raft->is_leader(); }
bool partition::has_followers() const { return _raft->has_followers(); }
void partition::block_new_leadership() const {
    return _raft->block_new_leadership();
}
void partition::unblock_new_leadership() const {
    return _raft->unblock_new_leadership();
}

ss::future<result<model::offset>> partition::linearizable_barrier() {
    return _raft->linearizable_barrier();
}

ss::future<std::error_code> partition::update_replica_set(
  std::vector<raft::broker_revision> brokers,
  model::revision_id new_revision_id) {
    return _raft->replace_configuration(std::move(brokers), new_revision_id);
}

ss::future<std::error_code> partition::update_replica_set(
  std::vector<raft::vnode> nodes,
  model::revision_id new_revision_id,
  std::optional<model::offset> learner_start_offset) {
    return _raft->replace_configuration(
      std::move(nodes), new_revision_id, learner_start_offset);
}

ss::future<std::error_code> partition::force_update_replica_set(
  std::vector<raft::vnode> voters,
  std::vector<raft::vnode> learners,
  model::revision_id new_revision_id) {
    return _raft->force_replace_configuration_locally(
      std::move(voters), std::move(learners), new_revision_id);
}

raft::group_configuration partition::group_configuration() const {
    return _raft->config();
}

model::revision_id partition::get_revision_id() const {
    return _raft->config().revision_id();
}
model::revision_id partition::get_log_revision_id() const {
    return _raft->log_config().get_revision();
}

std::optional<model::node_id> partition::get_leader_id() const {
    return _raft->get_leader_id();
}

std::optional<uint8_t> partition::get_under_replicated() const {
    return _raft->get_under_replicated();
}

model::offset partition::get_latest_configuration_offset() const {
    return _raft->get_latest_configuration_offset();
}

ss::lw_shared_ptr<const storage::offset_translator_state>
partition::get_offset_translator_state() const {
    return _raft->log()->get_offset_translator_state();
}

size_t partition::size_bytes() const { return _raft->log()->size_bytes(); }
size_t partition::reclaimable_size_bytes() const {
    return _raft->log()->reclaimable_size_bytes();
}

const storage::ntp_config& partition::get_ntp_config() const {
    return _raft->log_config();
}
model::term_id partition::get_term(model::offset o) const {
    return _raft->get_term(o);
}

ss::future<std::error_code>
partition::cancel_replica_set_update(model::revision_id rev) {
    return _raft->cancel_configuration_change(rev);
}

ss::future<std::error_code>
partition::force_abort_replica_set_update(model::revision_id rev) {
    return _raft->abort_configuration_change(rev);
}
consensus_ptr partition::raft() const { return _raft; }

ss::future<std::error_code> partition::set_writes_disabled(
  partition_properties_stm::writes_disabled disable,
  model::timeout_clock::time_point deadline) {
    ssx::rwlock::holder holder;
    auto lock_deadline = ss::semaphore::clock::now()
                         + ss::semaphore::clock::duration(
                           deadline - model::timeout_clock::now());
    try {
        holder = co_await _produce_lock.hold_write_lock(lock_deadline);
    } catch (ss::semaphore_timed_out&) {
        co_return errc::timeout;
    }
    if (!_feature_table.local().is_active(
          features::feature::partition_properties_stm)) {
        co_return errc::feature_disabled;
    }
    if (_partition_properties_stm == nullptr) {
        co_return errc::invalid_partition_operation;
    }
    if (disable && _rm_stm) {
        auto res = co_await _rm_stm->abort_all_txes();
        if (res != tx::errc::none) {
            co_return res;
        }
    }
    auto method = disable ? &partition_properties_stm::disable_writes
                          : &partition_properties_stm::enable_writes;
    co_return co_await (*_partition_properties_stm.*method)();
}

ss::future<result<ssx::rwlock_unit>> partition::hold_writes_enabled() {
    auto maybe_units = _produce_lock.attempt_read_lock();
    if (!maybe_units) {
        co_return errc::resource_is_being_migrated;
    }

    auto are_disabled
      = _partition_properties_stm
          ? co_await _partition_properties_stm->sync_writes_disabled()
          : partition_properties_stm::writes_disabled::no;
    if (!are_disabled.has_value()) {
        co_return are_disabled.error();
    }
    if (are_disabled.value()) {
        co_return errc::resource_is_being_migrated;
    }

    co_return *std::move(maybe_units);
}

} // namespace cluster

namespace seastar {

void lw_shared_ptr_deleter<cluster::partition>::dispose(cluster::partition* s) {
    delete s;
}

} // namespace seastar
