/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/ntp_archiver_service.h"

#include "base/vlog.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/archival/adjacent_segment_merger.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/archival_policy.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/replica_state_validator.h"
#include "cluster/archival/retention_calculator.h"
#include "cluster/archival/scrubber.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/types.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "net/connection.h"
#include "raft/fundamental.h"
#include "ssx/abort_source.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/future-util.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/ntp_config.h"
#include "storage/parser.h"
#include "utils/execution_monitor.h"
#include "utils/human.h"
#include "utils/lazy_abort_source.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <numeric>
#include <stdexcept>
#include <utility>

namespace {
constexpr auto housekeeping_jit = 5ms;
constexpr size_t max_bytes_rtc_node = 0x1000;
constexpr auto liveness_check_interval = 900s;
} // namespace

namespace archival {

namespace {

/// Return true if the exception is a shutdown error or a network error (e.g.
/// Broken pipe)
bool is_shutdown_or_disconnect(const std::exception_ptr& e) {
    return ssx::is_shutdown_exception(e) || net::is_disconnect_exception(e);
}

[[maybe_unused]] bool
is_nested_shutdown_exception(const ss::nested_exception& ex) {
    // During shutdown we could potentially get a 'shutdown' exception. If the
    // 'finally' continuation is used it will be invoked and if it touches the
    // gate or abort source it will also trigger a 'shutdown' exception. The
    // 'finally' continuation is invoked even for the exceptional future. In
    // this case we will get the nested_exception. If both exceptions are
    // shutdown exceptions we can safely conclude that the shutdown is in
    // progress (somewhat safely). If one of the exceptions is a shutdown
    // exception and another is a network disconnect exception we can also
    // conclude that the shutdown is in progress.
    return (is_shutdown_or_disconnect(ex.inner)
            && is_shutdown_or_disconnect(ex.outer))
           && (ssx::is_shutdown_exception(ex.inner) || ssx::is_shutdown_exception(ex.outer));
}

bool segment_meta_matches_stats(
  const cloud_storage::segment_meta& meta,
  const cloud_storage::segment_record_stats& stats,
  retry_chain_logger& ctxlog,
  bool gaps_allowed) {
    // Validate segment content. The 'stats' is computed when
    // the actual segment is scanned and represents the 'ground truth' about
    // its content. The 'meta' is the expected segment metadata. We
    // shouldn't replicate it if it doesn't match the 'stats'.
    const auto offsets_valid = [&] {
        if (gaps_allowed) {
            /**
             * Gap may be present either at the beginning or at the end of the
             * segment. So we check that the stats range is fully contained
             * within the segments range. Gaps should not influence the segment
             * size and number of config records.
             *
             * base_o                                last_o
             *   |           segment offsets           |
             *    <- gap ->                   <- gap ->
             *             |  stats offsets  |
             *
             *           base_o            last_o
             */

            return stats.base_rp_offset >= meta.base_offset
                   && stats.last_rp_offset <= meta.committed_offset;
        }
        return meta.base_offset == stats.base_rp_offset
               && meta.committed_offset == stats.last_rp_offset;
    };
    if (
      meta.size_bytes != stats.size_bytes || !offsets_valid()
      || static_cast<size_t>(meta.delta_offset_end - meta.delta_offset)
           != stats.total_conf_records) {
        vlog(
          ctxlog.error,
          "Metadata of the uploaded segment [size: {}, base: {}, last: {}, "
          "begin delta: {}, end delta: {}] "
          "doesn't match the segment [size: {}, base: {}, last: {}, total "
          "config records: {}]",
          meta.size_bytes,
          meta.base_offset,
          meta.committed_offset,
          meta.delta_offset,
          meta.delta_offset_end,
          stats.size_bytes,
          stats.base_rp_offset,
          stats.last_rp_offset,
          stats.total_conf_records);
        return false;
    }
    return true;
}

bool emit_read_write_fence(
  const ss::sharded<features::feature_table>& feature_table) {
    return !config::shard_local_cfg()
              .cloud_storage_disable_archival_stm_rw_fence.value()
           && feature_table.local().is_active(
             features::feature::cloud_storage_metadata_rw_fence);
}

cloud_storage::segment_meta convert_segment_meta(
  const segment_collector_stream& strm,
  const cluster::partition& parent,
  model::initial_revision_id rev,
  model::term_id archiver_term) {
    return cloud_storage::segment_meta{
      .is_compacted = strm.is_compacted,
      .size_bytes = strm.size,
      .base_offset = strm.start_offset,
      .committed_offset = strm.end_offset,
      .base_timestamp = strm.min_timestamp,
      .max_timestamp = strm.max_timestamp,
      // NOTE(oren): old code uses base - from_log_offset(base)
      // which is equivalent to base - (base - delta) = delta
      // so here we just use delta. Seems fine?
      .delta_offset = parent.log()->offset_delta(strm.start_offset),
      .ntp_revision = rev,
      .archiver_term = archiver_term,
      .segment_term = strm.term,
      .delta_offset_end = parent.log()->offset_delta(
        model::next_offset(strm.end_offset)),
      .sname_format = cloud_storage::segment_name_format::v3,
      /// Size of the tx-range (in v3 format)
      .metadata_size_hint = 0,
    };
}

struct one_time_stream_wrapper final : public stream_provider {
    std::optional<ss::input_stream<char>> stream;

    one_time_stream_wrapper(const one_time_stream_wrapper&) = delete;
    one_time_stream_wrapper(one_time_stream_wrapper&&) = default;
    one_time_stream_wrapper& operator=(const one_time_stream_wrapper&) = delete;
    one_time_stream_wrapper& operator=(one_time_stream_wrapper&&) = default;

    ~one_time_stream_wrapper() override = default;

    explicit one_time_stream_wrapper(ss::input_stream<char> s)
      : stream(std::move(s)) {}

    ss::input_stream<char> take_stream() override {
        vassert(stream.has_value(), "no stream to take");
        ss::input_stream<char> s = std::move(stream.value());
        stream = std::nullopt;
        return s;
    }

    ss::future<> close() override {
        if (stream.has_value()) {
            co_await stream.value().close();
        }
        co_return;
    }
};
} // namespace

ntp_archiver_upload_result::ntp_archiver_upload_result(
  cloud_storage::upload_result r)
  : _result(r) {}

// Success result
ntp_archiver_upload_result::ntp_archiver_upload_result(
  const cloud_storage::segment_record_stats& m)
  : _stats(m)
  , _result(cloud_storage::upload_result::success) {}

ntp_archiver_upload_result ntp_archiver_upload_result::merge(
  const std::vector<ntp_archiver_upload_result>& results) {
    vassert(
      !results.empty(),
      "list of ntp_archiver_upload_result values can't be empty");
    auto res = cloud_storage::upload_result::success;
    std::optional<cloud_storage::segment_record_stats> stats;
    for (auto& r : results) {
        if (r.has_record_stats()) {
            stats = r.record_stats();
        }
        if (r.result() != cloud_storage::upload_result::success) {
            res = r.result();
        }
    }
    if (stats && res == cloud_storage::upload_result::success) {
        return ntp_archiver_upload_result(stats.value());
    }
    vassert(
      res != cloud_storage::upload_result::success,
      "success result should have record stats set");
    return res;
}

bool ntp_archiver_upload_result::has_record_stats() const {
    return _stats.has_value();
}

const cloud_storage::segment_record_stats&
ntp_archiver_upload_result::record_stats() const {
    return _stats.value();
}

cloud_storage::upload_result ntp_archiver_upload_result::result() const {
    return _result;
}

cloud_storage::upload_result
ntp_archiver_upload_result::operator()(cloud_storage::upload_result) const {
    return _result;
}

namespace {
std::unique_ptr<adjacent_segment_merger> maybe_make_adjacent_segment_merger(
  ntp_archiver& self, const storage::ntp_config& cfg) {
    std::unique_ptr<adjacent_segment_merger> result = nullptr;
    if (cfg.is_archival_enabled() && !cfg.is_read_replica_mode_enabled()) {
        result = std::make_unique<adjacent_segment_merger>(
          self,
          true,
          config::shard_local_cfg()
            .cloud_storage_enable_segment_merging.bind());
    }
    return result;
}

std::unique_ptr<scrubber> maybe_make_scrubber(
  ntp_archiver& self,
  cloud_storage::remote& remote,
  features::feature_table& feature_table,
  const storage::ntp_config& cfg) {
    std::unique_ptr<scrubber> result = nullptr;
    if (!cfg.is_read_replica_mode_enabled()) {
        result = std::make_unique<scrubber>(
          self,
          remote,
          feature_table,
          config::shard_local_cfg().cloud_storage_enable_scrubbing.bind(),
          config::shard_local_cfg()
            .cloud_storage_partial_scrub_interval_ms.bind(),
          config::shard_local_cfg().cloud_storage_full_scrub_interval_ms.bind(),
          config::shard_local_cfg()
            .cloud_storage_scrubbing_interval_jitter_ms.bind());
    }
    return result;
}

std::vector<std::exception_ptr> flatten_exception(const std::exception_ptr& e) {
    std::vector<std::exception_ptr> result;
    chunked_vector<std::exception_ptr> stk;
    stk.push_back(e);
    while (!stk.empty()) {
        auto curr = stk.back();
        stk.pop_back();
        try {
            std::rethrow_exception(curr);
        } catch (const ss::nested_exception& e) {
            // outermost exception to front of list
            stk.push_back(e.inner);
            stk.push_back(e.outer);
        } catch (...) {
            result.push_back(std::current_exception());
        }
    }
    return result;
}

} // namespace

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp,
  ss::lw_shared_ptr<const configuration> conf,
  cloud_storage::remote& remote,
  cloud_io::cache& c,
  cluster::partition& parent,
  ss::shared_ptr<cloud_storage::async_manifest_view> amv)
  : _ntp(ntp.ntp())
  , _rev(ntp.get_remote_revision())
  , _remote(remote)
  , _cache(c)
  , _parent(parent)
  , _policy(_ntp, conf->time_limit)
  , _gate()
  , _rtctx("ntp_archiver", _as, max_bytes_rtc_node)
  , _rtcnode(_rtctx)
  , _rtclog(archival_log, _rtcnode, _ntp.path())
  , _conf(conf)
  , _sync_manifest_timeout(
      config::shard_local_cfg()
        .cloud_storage_readreplica_manifest_sync_timeout_ms.bind())
  , _max_segments_pending_deletion(
      config::shard_local_cfg()
        .cloud_storage_max_segments_pending_deletion_per_partition.bind())
  , _gc_max_segments(
      config::shard_local_cfg().cloud_storage_gc_max_segments_per_run.bind())
  , _housekeeping_interval(
      config::shard_local_cfg().cloud_storage_housekeeping_interval_ms.bind())
  , _housekeeping_jitter(_housekeeping_interval(), housekeeping_jit)
  , _next_housekeeping(_housekeeping_jitter())
  , _feature_table(parent.feature_table())
  , _local_segment_merger(
      maybe_make_adjacent_segment_merger(*this, _parent.log()->config()))
  , _scrubber(maybe_make_scrubber(
      *this, _remote, _feature_table.local(), _parent.log()->config()))
  , _manifest_upload_interval(
      config::shard_local_cfg()
        .cloud_storage_manifest_max_upload_interval_sec.bind())
  , _manifest_view(std::move(amv))
  , _initial_backoff(
      config::shard_local_cfg()
        .cloud_storage_upload_loop_initial_backoff_ms.bind())
  , _max_backoff(
      config::shard_local_cfg().cloud_storage_upload_loop_max_backoff_ms.bind())
  , _execution_monitor("ntp_archiver", liveness_check_interval) {
    _housekeeping_interval.watch([this] {
        _housekeeping_jitter = simple_time_jitter<ss::lowres_clock>{
          _housekeeping_interval(), housekeeping_jit};
        _next_housekeeping = _housekeeping_jitter();
    });

    if (_local_segment_merger) {
        _local_segment_merger->set_enabled(false);
    }
    if (_scrubber) {
        _scrubber->set_enabled(false);
    }

    _start_term = _parent.term();
    // Override bucket for read-replica
    if (_parent.is_read_replica_mode_enabled()) {
        _bucket_override = _parent.get_read_replica_bucket();
    }
}

archival_stm_fence ntp_archiver::emit_rw_fence() {
    return {
      .read_write_fence
      = _parent.archival_meta_stm()->manifest().get_applied_offset(),
      // Only use the rw-fence if the feature is enabled which requires
      // major version upgrade.
      .emit_rw_fence_cmd = emit_read_write_fence(_feature_table),
    };
}

ss::future<std::error_code>
ntp_archiver::maybe_repair_manifest(ss::lowres_clock::time_point deadline) {
    auto repaired_copy = _parent.archival_meta_stm()->manifest().repair_state();
    if (!repaired_copy) {
        co_return std::error_code{};
    }

    vlog(
      _rtclog.warn, "Manifest repair created a new manifest. Replicating it.");
    auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
    auto fence = emit_rw_fence();
    if (fence.emit_rw_fence_cmd) {
        vlog(
          _rtclog.debug,
          "replace manifest with repair, read-write fence: {}",
          fence.read_write_fence);
        batch.read_write_fence(fence.read_write_fence);
    }

    batch.replace_manifest(repaired_copy->to_iobuf());
    auto ec = co_await batch.replicate();
    if (ec) {
        vlog(
          _rtclog.error,
          "Failed to replace manifest with repaired version: {}",
          ec);
    } else {
        vlog(
          _rtclog.debug, "Finished replacing manifest with repaired version");
    }

    co_return ec;
}

void ntp_archiver::log_collected_traces() noexcept {
    try {
        _rtclog.bypass_tracing([this] {
            _rtclog.error("Diagnostic dump start");

            _rtclog.info("[repeat] start");
            if (_rtctx.truncation_warning()) {
                _rtclog.info("[truncated]");
            }
            for (const auto& trace : _rtctx.traces()) {
                _rtclog.info("[repeat] {}", trace);
            }
            _rtclog.info("[repeat] end");

            // Log timestamps of operations
            _rtclog.info(
              "last manifest upload time: {}, last_segment_upload_time: {}, "
              "last_marked_clean_time: {}, last_upload_time: {}, "
              "last_sync_time: "
              "{}",
              _last_manifest_upload_time.time_since_epoch(),
              _last_segment_upload_time.time_since_epoch(),
              _last_marked_clean_time.time_since_epoch(),
              _last_upload_time.time_since_epoch(),
              _last_sync_time.has_value() ? _last_sync_time->time_since_epoch()
                                          : ss::lowres_clock::duration{});

            // Log mutexes
            auto mutex_cp = _mutex.get_blocking_checkpoint();
            if (mutex_cp) {
                auto delta = std::chrono::steady_clock::now()
                             - mutex_cp.value().time;
                _rtclog.info(
                  "mutex units: {}, {}ms",
                  mutex_cp.value().line,
                  std::chrono::duration_cast<std::chrono::milliseconds>(delta));
            }
            auto uploads_active_cp = _uploads_active.get_blocking_checkpoint();
            if (uploads_active_cp) {
                auto delta = std::chrono::steady_clock::now()
                             - uploads_active_cp.value().time;
                _rtclog.info(
                  "uploads_active units: {}, {}ms",
                  uploads_active_cp.value().line,
                  std::chrono::duration_cast<std::chrono::milliseconds>(delta));
            }

            // Log raft state
            _rtclog.info(
              "raft term: {}, is_leader: {}, offsets: {}",
              _parent.term(),
              _parent.is_leader(),
              _parent.raft()->log()->offsets());

            // Log manifest state
            _rtclog.info(
              "manifest, last uploaded offset {}, last uploaded compacted "
              "offset "
              "{}, applied offset {}, insync offset {}, last scrubbed offset "
              "{}, "
              "archive start offset {}, archive start delta {}, archive clean "
              "offset "
              "{}, last segment {}",
              manifest().get_last_offset(),
              manifest().get_last_uploaded_compacted_offset(),
              manifest().get_applied_offset(),
              manifest().get_insync_offset(),
              manifest().last_scrubbed_offset().value_or(model::offset()),
              manifest().get_archive_start_offset(),
              manifest().get_archive_start_offset_delta(),
              manifest().get_archive_clean_offset(),
              manifest().empty()
                ? "N/A"
                : ssx::sformat("{}", manifest().last_segment()));
            _rtclog.info("Diagnostic dump end");
        });
    } catch (...) {
        vlog(
          _rtclog.error,
          "Failed to log diagnostic information: {}",
          std::current_exception());
    }
}

const cloud_storage::partition_manifest& ntp_archiver::manifest() const {
    vassert(
      _parent.archival_meta_stm(),
      "Archival STM is not available for {}",
      _ntp.path());
    return _parent.archival_meta_stm()->manifest();
}

ss::future<> ntp_archiver::start() {
    if (_parent.get_ntp_config().is_read_replica_mode_enabled()) {
        ssx::spawn_with_gate(_gate, [this] {
            return sync_manifest_until_abort().then([this] {
                if (!_as.abort_requested()) {
                    vlog(
                      _rtclog.error,
                      "Sync loop stopped without an abort being requested. "
                      "Please disable and re-enable "
                      "redpanda.remote.readreplica "
                      "the topic in order to restart it.");
                }
            });
        });
    } else {
        ssx::spawn_with_gate(_gate, [this] {
            return upload_until_abort().then([this]() {
                if (!_as.abort_requested()) {
                    vlog(
                      _rtclog.error,
                      "Upload loop stopped without an abort being requested. "
                      "Please disable and re-enable redpanda.remote.write "
                      "the topic in order to restart it.");
                }
            });
        });
    }

    ssx::execution_monitor::stall_detector_config sdc{
      .cb =
        [this](const retry_chain_context&) {
            vlog(
              _rtclog.error, "stall detected, logging diagnostic information");
            log_collected_traces();
        },
      .contexts = {&_rtctx}};

    ssx::execution_monitor::unexpected_shutdown_detector_config udc{
      .cb =
        [this] {
            vlog(
              _rtclog.error,
              "unexpected shutdown detected, logging diagnostic information");
            log_collected_traces();
        },
      .gate = &_gate,
    };

    _execution_monitor.start(sdc, udc);

    co_return;
}

void ntp_archiver::notify_leadership(std::optional<model::node_id> leader_id) {
    bool is_leader = leader_id && *leader_id == _parent.raft()->self().id();
    vlog(
      _rtclog.debug,
      "notify_leadership: is_leader={}, leader_id={}, raft group id={}",
      is_leader,
      leader_id ? *leader_id : model::node_id{},
      _parent.raft()->self().id());
    if (is_leader) {
        _leader_cond.signal();
    }
}

ss::future<> ntp_archiver::upload_until_abort() {
    if (
      unlikely(
        config::shard_local_cfg()
          .cloud_storage_disable_upload_loop_for_tests.value())) {
        vlog(_rtclog.warn, "Skipping upload loop start");
        co_return;
    }
    if (!_probe.has_value()) {
        initialize_probe();
    }

    while (!_as.abort_requested()) {
        if (!_parent.is_leader() || _paused) {
            bool shutdown = false;
            try {
                vlog(
                  _rtclog.debug, "upload loop waiting for leadership/unpause");
                constexpr auto leader_cond_timeout = 30s;
                _rtctx.suspend_to(
                  ss::lowres_clock::now() + leader_cond_timeout + 1s);
                co_await _leader_cond.wait(leader_cond_timeout);
            } catch (const ss::condition_variable_timed_out&) {
                continue;
            } catch (const ss::broken_condition_variable&) {
                // stop() was called
                shutdown = true;
            }

            if (shutdown || _as.abort_requested()) {
                vlog(_rtclog.trace, "upload loop shutting down");
                break;
            }

            // We were signalled that we became leader: fall through and
            // start the upload loop.
        }

        _start_term = _parent.term();

        // If a flush is in progress from a previous term, reset the flush
        // uploads offset and signal to the condition variable.
        if (flush_in_progress()) {
            vlog(
              _rtclog.debug,
              "Flush from previous term was in progress, resetting.");
            _flush_uploads_offset.reset();
            _flush_cond.broadcast();
        }

        if (!may_begin_uploads()) {
            continue;
        }
        vlog(_rtclog.debug, "upload loop starting in term {}", _start_term);
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto is_synced = co_await _parent.archival_meta_stm()->sync(
          sync_timeout);
        if (!is_synced.has_value()) {
            continue;
        }

        replica_state_validator validator(
          *_parent.log(), _parent.archival_meta_stm()->manifest());
        if (validator.has_anomalies()) {
            // The validation and printing of anomalies is happening
            // every time we start an archiver
            validator.maybe_print_scarry_log_message();

            auto ok_to_skip = [&] {
                // The gap anomalie is expected if the
                // 'is_remote_allow_gaps_enabled' is true (either because the
                // topic property or cluster global config).
                int gap_anomalies = 0;
                int ots_anomalies = 0;
                for (const auto& anomaly : validator.get_anomalies()) {
                    switch (anomaly.type) {
                    case replica_state_anomaly_type::offsets_gap:
                        gap_anomalies++;
                        break;
                    case replica_state_anomaly_type::ot_state:
                        ots_anomalies++;
                        break;
                    }
                }
                if (ots_anomalies > 0) {
                    return false;
                }
                return gap_anomalies == 0
                       || _parent.get_ntp_config()
                            .is_remote_allow_gaps_enabled();
            }();

            auto checks_disabled
              = config::shard_local_cfg()
                  .cloud_storage_disable_upload_consistency_checks();

            // Disable uploads and housekeeping if consistency
            // checks are not disabled
            if (!checks_disabled && !ok_to_skip) {
                // Consistency checks will not let us do anything anyway
                vlog(
                  _rtclog.warn,
                  "upload loop stalled for {}ms in term {} because of "
                  "anomalies",
                  sync_timeout.count(),
                  _start_term);
                auto hold = _probe.value().register_archiver_on_hold(true);
                co_await ss::sleep_abortable(sync_timeout, _as);
                continue;
            } else {
                vlog(
                  _rtclog.info,
                  // The list of anomalies is logged separately
                  "upload loop continuing in term {} with anomalies, "
                  "consistency checks disabled: {}, gap anomalies are skipped: "
                  "{}",
                  _start_term,
                  checks_disabled,
                  ok_to_skip);
            }
        }

        if (
          auto ec = co_await maybe_repair_manifest(
            ss::lowres_clock::now() + sync_timeout);
          ec) {
            vlog(_rtclog.warn, "Failed to repair manifest: {}, retrying", ec);
            continue;
        }

        vlog(_rtclog.debug, "upload loop synced in term {}", _start_term);
        if (!may_begin_uploads()) {
            continue;
        }

        if (_local_segment_merger) {
            auto is_compacted = _parent.log()->config().is_locally_compacted();
            if (!is_compacted) {
                vlog(
                  _rtclog.debug,
                  "Enable adjacent segment merger in term {}, log config: {}",
                  _start_term,
                  _parent.log()->config());
                _local_segment_merger->set_enabled(true);
            }
        }
        if (_scrubber) {
            vlog(_rtclog.debug, "Enable scrubber in term {}", _start_term);
            _scrubber->set_enabled(true);
        }
        auto disable_hk_jobs = ss::defer([this] {
            if (_local_segment_merger) {
                vlog(
                  _rtclog.debug,
                  "Disable adjacent segment merger in term {}",
                  _start_term);
                _local_segment_merger->set_enabled(false);
            }
            if (_scrubber) {
                vlog(_rtclog.debug, "Disable scrubber in term {}", _start_term);
                _scrubber->set_enabled(false);
            }
        });

        auto do_upload = [this] { return upload_until_term_change_legacy(); };

        co_await ss::with_scheduling_group(
          _conf->upload_scheduling_group, do_upload)
          .handle_exception_type([](const ss::abort_requested_exception&) {})
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([](const ss::broken_semaphore&) {})
          .handle_exception_type([](const ss::broken_named_semaphore&) {})
          .handle_exception_type([this](const ss::semaphore_timed_out& e) {
              vlog(
                _rtclog.warn,
                "Semaphore timed out in the upload loop: {}. This may be "
                "due to the system being overloaded. The loop will "
                "restart.",
                e);
          })
          .handle_exception([this](std::exception_ptr e) {
              vlog(_rtclog.error, "upload loop error: {}", e);
          });

        if (flush_in_progress()) {
            vlog(
              _rtclog.debug,
              "Exited upload_until_term_change() loop, alerting flush "
              "waiters.");
            _flush_uploads_offset.reset();
            _flush_cond.broadcast();
        }
    }
}

ss::future<> ntp_archiver::sync_manifest_until_abort() {
    if (
      unlikely(
        config::shard_local_cfg()
          .cloud_storage_disable_read_replica_loop_for_tests.value())) {
        vlog(_rtclog.warn, "Skipping read replica sync loop start");
        co_return;
    }
    if (!_probe.has_value()) {
        initialize_probe();
    }

    while (!_as.abort_requested()) {
        if (!_parent.is_leader() || _paused) {
            bool shutdown = false;
            try {
                vlog(
                  _rtclog.debug,
                  "sync manifest loop waiting for leadership/unpause");
                constexpr auto leader_cond_timeout = 30s;
                _rtctx.suspend_to(
                  ss::lowres_clock::now() + leader_cond_timeout + 1s);
                co_await _leader_cond.wait(leader_cond_timeout);
            } catch (const ss::condition_variable_timed_out&) {
                continue;
            } catch (const ss::broken_condition_variable&) {
                shutdown = true;
            }

            if (shutdown || _as.abort_requested()) {
                // stop() was called
                vlog(_rtclog.trace, "sync manifest loop shutting down");
                break;
            }

            // We were signalled that we became leader: fall through and
            // start the upload loop.
        }

        _start_term = _parent.term();
        if (!can_update_archival_metadata()) {
            continue;
        }

        vlog(
          _rtclog.debug, "sync manifest loop starting in term {}", _start_term);

        try {
            co_await sync_manifest_until_term_change()
              .handle_exception_type(
                [](const ss::abort_requested_exception&) {})
              .handle_exception_type([](const ss::broken_semaphore&) {})
              .handle_exception_type([](const ss::broken_named_semaphore&) {})
              .handle_exception_type([](const ss::gate_closed_exception&) {});
        } catch (const ss::semaphore_timed_out& e) {
            vlog(
              _rtclog.warn,
              "Semaphore timed out in the upload loop: {}. This may be "
              "due to the system being overloaded. The loop will "
              "restart.",
              e);
        } catch (...) {
            log_collected_traces();
            vlog(
              _rtclog.error,
              "sync manifest loop error: {}",
              std::current_exception());
        }
    }
}

ss::future<> ntp_archiver::upload_topic_manifest() {
    auto topic_cfg_opt = _parent.get_topic_config();
    if (!topic_cfg_opt) {
        // This is unexpected: by the time partition_manager instantiates
        // partitions, they should have had their configs loaded by controller
        // backend.
        vlog(
          _rtclog.error,
          "No topic configuration available for {}",
          _parent.ntp());
        co_return;
    }

    auto& topic_cfg = *topic_cfg_opt;

    vlog(
      _rtclog.debug,
      "Uploading topic manifest for {}, topic config {}",
      _parent.ntp(),
      topic_cfg);

    auto replication_factor = cluster::replication_factor(
      _parent.raft()->config().current_config().voters.size());

    try {
        retry_chain_node fib(
          _conf->manifest_upload_timeout(),
          _conf->cloud_storage_initial_backoff(),
          &_rtcnode);
        retry_chain_logger ctxlog(archival_log, fib);
        vlog(ctxlog.info, "Uploading topic manifest {}", _parent.ntp());
        auto cfg_copy = topic_cfg.get();
        cfg_copy.replication_factor = replication_factor;
        cloud_storage::topic_manifest tm(cfg_copy, _rev);
        auto key = tm.get_manifest_path(remote_path_provider());
        vlog(ctxlog.debug, "Topic manifest object key is '{}'", key);
        auto res = co_await _remote.upload_manifest(
          _conf->bucket_name, tm, key, fib);
        if (res != cloud_storage::upload_result::success) {
            vlog(ctxlog.warn, "Topic manifest upload failed: {}", key);
        } else {
            _topic_manifest_dirty = false;
        }
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::abort_requested_exception&) {
    } catch (...) {
        vlog(
          _rtclog.warn,
          "Error writing topic manifest for {}: {}",
          _parent.ntp(),
          std::current_exception());
    }
}

ss::future<bool> ntp_archiver::sync_for_tests() {
    while (!_as.abort_requested()) {
        if (!_parent.is_leader()) {
            bool shutdown = false;
            try {
                vlog(_rtclog.debug, "test waiting for leadership");
                co_await _leader_cond.wait();
            } catch (const ss::broken_condition_variable&) {
                // stop() was called
                shutdown = true;
            }

            if (shutdown || _as.abort_requested()) {
                vlog(_rtclog.trace, "sync_for_tests shutting down");
                co_return false;
            }
        }
        _start_term = _parent.term();
        if (!can_update_archival_metadata()) {
            co_return false;
        }
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        if (co_await _parent.archival_meta_stm()->sync(sync_timeout)) {
            co_return true;
        }
    }
    co_return false;
}

ss::future<std::error_code> ntp_archiver::process_anomalies(
  model::timestamp scrub_timestamp,
  std::optional<model::offset> last_scrubbed_offset,
  cloud_storage::scrub_status status,
  cloud_storage::anomalies detected,
  ss::abort_source& caller_as) {
    ssx::composite_abort_source cas{caller_as, _as};

    // If there's ongoing housekeeping job, let it finish first.
    auto units = co_await _mutex.get_units(cas.as());

    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto deadline = ss::lowres_clock::now() + sync_timeout;

    auto error = co_await _parent.archival_meta_stm()->process_anomalies(
      scrub_timestamp,
      last_scrubbed_offset,
      status,
      std::move(detected),
      deadline,
      cas.as());
    if (error != cluster::errc::success) {
        vlog(
          _rtclog.warn,
          "Failed to replicate process anomalies command: {}",
          error.message());
    }

    co_return error;
}

ss::future<std::error_code> ntp_archiver::reset_scrubbing_metadata() {
    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto deadline = ss::lowres_clock::now() + sync_timeout;
    auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
    batch.reset_scrubbing_metadata();
    auto error = co_await batch.replicate();

    if (error != cluster::errc::success) {
        vlog(
          _rtclog.warn,
          "Failed to replicate reset scrubbing metadata command: {}",
          error.message());
    } else if (_scrubber) {
        _scrubber->reset_scheduler();
    }

    co_return error;
}

ss::future<> ntp_archiver::maybe_complete_flush() {
    // If a flush was in progress and the archiver has uploaded past the set
    // _flush_uploads_offset, there should be a number of actions taken to
    // fully complete the flush operation.
    // 1. Upload topic manifest (if we are partition 0).
    // 2. Upload the partition manifest.
    // 3. Flush manifest to the clean offset.
    // 4. Reset flush state.
    // 5. Broadcast using flush cv to alert all waiters.
    if (uploaded_data_past_flush_offset()) {
        if (_parent.ntp().tp.partition == 0) {
            co_await upload_topic_manifest();
        }

        // Attempt to upload the manifest. Flush is considered complete upon
        // success. If unsuccessful, this will be retried on next iteration
        // of background loop.
        if (
          co_await upload_manifest(upload_loop_flush_complete_ctx_label)
          == cloud_storage::upload_result::success) {
            co_await flush_manifest_clean_offset();
            _flush_uploads_offset.reset();
            _flush_cond.broadcast();
        }
    }
}

ss::future<> ntp_archiver::upload_until_term_change_legacy() {
    auto backoff = _conf->upload_loop_initial_backoff();

    if (!_feature_table.local().is_active(
          features::feature::cloud_storage_manifest_format_v2)) {
        vlog(
          _rtclog.warn,
          "Cannot operate upload loop during upgrade, not all nodes "
          "are upgraded yet.  Waiting...");
        co_await _feature_table.local().await_feature(
          features::feature::cloud_storage_manifest_format_v2, _as);
        vlog(
          _rtclog.info, "Upgrade complete, proceeding with the upload loop.");

        // The cluster likely needed a bunch of restarts in order to
        // reach this point, which means that leadership may have been
        // transferred away (hence the explicit check).
        if (!may_begin_uploads()) {
            co_return;
        }
    }

    // Before starting, upload the manifest if needed.  This makes our
    // behavior more deterministic on first start (uploading the empty
    // manifest) and after unclean leadership changes (flush dirty manifest
    // as soon as we can, rather than potentially waiting for segment
    // uploads).
    {
        auto units = co_await _uploads_active.get_units();
        co_await maybe_upload_manifest(upload_loop_prologue_ctx_label);
        co_await flush_manifest_clean_offset();
    }

    while (may_begin_uploads()) {
        // Reset trace logging in the beginning of the upload round
        _rtctx.reset();
        // Hold sempahore units to enable other code to know that we are in
        // the process of doing uploads + wait for us to drop out if they
        // e.g. set _paused.
        vassert(!_paused, "may_begin_uploads must ensure !_paused");
        auto units = co_await _uploads_active.get_units();
        vlog(
          _rtclog.trace,
          "upload_until_term_change: got units (current {}), paused={}",
          _uploads_active.has_units(),
          _paused);

        // Bump up archival STM's state to make sure that it's not lagging
        // behind too far. If the STM is lagging behind we will have to read a
        // lot of data next time we upload something.
        vassert(
          _parent.archival_meta_stm(),
          "Upload loop: archival metadata STM is not created for {} archiver",
          _ntp.path());

        if (_parent.ntp().tp.partition == 0 && _topic_manifest_dirty) {
            co_await upload_topic_manifest();
        }

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto is_synced = co_await _parent.archival_meta_stm()->sync(
          sync_timeout);
        if (!is_synced.has_value()) {
            // This can happen on leadership changes, or on timeouts waiting
            // for stm to catch up: in either case, we should re-check our
            // loop condition: we will drop out if lost leadership, otherwise
            // we will end up back here to try the sync again.
            continue;
        }

        // This is the offset of the last applied command. It is used
        // as a fence to implement optimistic concurrency control.
        auto fence
          = _parent.archival_meta_stm()->manifest().get_applied_offset();
        vlog(
          _rtclog.debug,
          "fence value is: {}, in-sync offset: {}",
          fence,
          _parent.archival_meta_stm()->get_insync_offset());

        bool uploads_paused
          = !config::shard_local_cfg().cloud_storage_enable_segment_uploads();
        std::optional<batch_result> result;
        auto track_paused = _probe.value().register_archiver_on_hold(
          uploads_paused);
        if (!uploads_paused) {
            result = co_await upload_next_candidates(emit_rw_fence());
        }
        if (result.has_value()) {
            auto [compacted_upload_result, non_compacted_upload_result]
              = result.value();
            if (non_compacted_upload_result.num_failed != 0) {
                // The logic in class `remote` already does retries: if we get
                // here, it means the upload failed after several retries,
                // justifying a warning to the operator: something non-transient
                // may be wrong, although we do also see this in practice on AWS
                // S3 occasionally during normal operation.
                vlog(
                  _rtclog.warn,
                  "Failed to upload {} segments out of {}",
                  non_compacted_upload_result.num_failed,
                  non_compacted_upload_result.num_succeeded
                    + non_compacted_upload_result.num_failed
                    + non_compacted_upload_result.num_cancelled);
            } else if (non_compacted_upload_result.num_succeeded != 0) {
                vlog(
                  _rtclog.debug,
                  "Successfully uploaded {} segments",
                  non_compacted_upload_result.num_succeeded);
            }

            if (non_compacted_upload_result.num_cancelled != 0) {
                vlog(
                  _rtclog.debug,
                  "Cancelled upload of {} segments",
                  non_compacted_upload_result.num_cancelled);
            }
        }

        if (ss::lowres_clock::now() >= _next_housekeeping) {
            if (co_await housekeeping() == housekeeping_result::partial) {
                // Only apply the very short jitter if there is more pending
                // housekeeping work to do.
                // NB: The actual duration between housekeeping runs is
                // bounded by upload loop scheduling. Here we establish a lower
                // bound on duration between housekeeping runs.
                _next_housekeeping = ss::lowres_clock::now() + housekeeping_jit;
            } else {
                _next_housekeeping = _housekeeping_jitter();
            }
        }

        if (!may_begin_uploads()) {
            break;
        }

        // If flush is in progress complete it first.
        co_await maybe_complete_flush();

        // This is the fallback path for uploading manifest if it didn't happen
        // inline with segment uploads: this path will be taken on e.g. restarts
        // or unclean leadership changes.
        if (co_await maybe_upload_manifest(upload_loop_epilogue_ctx_label)) {
            co_await flush_manifest_clean_offset();
        } else {
            // No manifest upload, but if some background task had incremented
            // the projected clean offset without flushing it, flush it for
            // them.
            co_await maybe_flush_manifest_clean_offset();
        }

        // Drop _uploads_active lock: we are not considered active while
        // sleeping for backoff at the end of the loop.
        units.release();
        vlog(
          _rtclog.trace,
          "upload_until_term_change: released units (current {})",
          _uploads_active.has_units());

        if (
          !result.has_value()
          || result.value().non_compacted_upload_result.num_succeeded == 0) {
            // The backoff algorithm here is used to prevent high CPU
            // utilization when redpanda is not receiving any data and there
            // is nothing to update. Also, we want to limit amount of
            // logging if nothing is uploaded because of bad configuration
            // or some other problem.
            //
            // We want to limit max backoff duration
            // to some reasonable value (e.g. 5s) because otherwise it can
            // grow very large disabling the archival storage
            vlog(
              _rtclog.trace, "Nothing to upload, applying backoff algorithm");
            auto timeout = backoff + _backoff_jitter.next_jitter_duration();
            // Reset the context before sleeping to avoid holding to the memory.
            // Most of the time the archiver spends in the following 'wait'
            // call. It's not possible for it to get stalled there so it's safe
            // to reset the context here. This guarantees that we only consuming
            // additional memory for traces for the duration of the upload.
            _rtctx.reset();
            _rtctx.suspend_to(ss::lowres_clock::now() + timeout + 1s);
            co_await _wakeup_event.wait(timeout);
            backoff = std::min(backoff * 2, _conf->upload_loop_max_backoff());
        } else {
            backoff = _conf->upload_loop_initial_backoff();
        }
    }
}

// Target segment size + min acceptable size
struct segment_size_limits {
    size_t target;
    size_t lowest;
};

ss::future<> ntp_archiver::sync_manifest_until_term_change() {
    while (can_update_archival_metadata()) {
        _rtctx.reset();

        if (!_feature_table.local().is_active(
              features::feature::cloud_storage_manifest_format_v2)) {
            vlog(
              _rtclog.warn,
              "Cannot synchronize read replica during upgrade, not all nodes "
              "are upgraded yet.  Waiting...");
            co_await _feature_table.local().await_feature(
              features::feature::cloud_storage_manifest_format_v2, _as);
            vlog(
              _rtclog.info,
              "Upgrade complete, proceeding to sync read replica.");

            // Go around the loop to check we are still eligible to do the
            // update
            continue;
        }

        cloud_storage::download_result result = co_await sync_manifest();

        if (result != cloud_storage::download_result::success) {
            // The logic in class `remote` already does retries: if we get here,
            // it means the download failed after several retries, indicating
            // something non-transient may be wrong. Hence error severity.
            vlog(
              _rtclog.error,
              "Failed to download manifest {}",
              manifest().get_manifest_path(remote_path_provider()));
        } else {
            vlog(
              _rtclog.debug,
              "Successfuly downloaded manifest {}",
              manifest().get_manifest_path(remote_path_provider()));
        }
        _rtctx.suspend_to(
          ss::lowres_clock::now() + _sync_manifest_timeout() + 1s);
        co_await ss::sleep_abortable(_sync_manifest_timeout(), _as);
    }
}

ss::future<cloud_storage::download_result> ntp_archiver::sync_manifest() {
    vlog(_rtclog.debug, "Downloading manifest in read-replica mode");
    auto [m, res] = co_await download_manifest();
    if (res != cloud_storage::download_result::success) {
        vlog(
          _rtclog.error,
          "Failed to download partition manifest in read-replica mode");
        co_return res;
    } else {
        if (m == _parent.archival_meta_stm()->manifest()) {
            // TODO: This can be made more efficient by using a conditional GET:
            // https://github.com/redpanda-data/redpanda/issues/11776
            //
            // Then, the GET can be adapted to return the raw buffer, so that we
            // don't go through a deserialize/serialize cycle before writing the
            // manifest back into a raft batch.
            vlog(_rtclog.debug, "Manifest has not changed, no sync required");
            co_return res;
        }

        vlog(
          _rtclog.debug,
          "Updating the archival_meta_stm in read-replica mode, in-sync "
          "offset: {}, last uploaded offset: {}, last compacted offset: {}",
          m.get_insync_offset(),
          m.get_last_offset(),
          m.get_last_uploaded_compacted_offset());

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto serialized = m.to_iobuf();
        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        builder.replace_manifest(m.to_iobuf());

        auto errc = co_await builder.replicate();
        if (errc) {
            if (errc == raft::errc::shutting_down) {
                // During shutdown, act like we hit an abort source rather
                // than trying to log+handle this like a write error.
                throw ss::abort_requested_exception();
            }

            vlog(
              _rtclog.warn,
              "Can't replicate archival_metadata_stm configuration batch: "
              "{}",
              errc.message());
            co_return cloud_storage::download_result::failed;
        }
    }

    _last_sync_time = ss::lowres_clock::now();
    co_return cloud_storage::download_result::success;
}

bool ntp_archiver::can_update_archival_metadata() const {
    return !_as.abort_requested() && !_gate.is_closed() && _parent.is_leader()
           && _parent.term() == _start_term;
}

bool ntp_archiver::may_begin_uploads() const {
    return can_update_archival_metadata() && !_paused;
}

ss::future<> ntp_archiver::stop() {
    if (_local_segment_merger) {
        if (!_local_segment_merger->interrupted()) {
            _local_segment_merger->interrupt();
        }
        co_await _local_segment_merger.get()->stop();
    }

    if (_scrubber) {
        if (!_scrubber->interrupted()) {
            _scrubber->interrupt();
        }
        co_await _scrubber->stop();
    }

    _execution_monitor.stop();

    _as.request_abort();
    _uploads_active.broken();
    _leader_cond.broken();
    _flush_cond.broken();
    _wakeup_event.broken();

    co_await _gate.close();
    _probe.reset();
}

const model::ntp& ntp_archiver::get_ntp() const { return _ntp; }

model::initial_revision_id ntp_archiver::get_revision_id() const {
    return _rev;
}

std::optional<ss::lowres_clock::time_point>
ntp_archiver::get_last_sync_time() const {
    return _last_sync_time;
}

ss::future<
  std::pair<cloud_storage::partition_manifest, cloud_storage::download_result>>
ntp_archiver::download_manifest() {
    auto guard = _gate.hold();
    retry_chain_node fib(
      _conf->manifest_upload_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &_rtcnode);
    cloud_storage::partition_manifest tmp(_ntp, _rev);
    vlog(_rtclog.debug, "Downloading manifest");
    cloud_storage::partition_manifest_downloader dl(
      get_bucket_name(), remote_path_provider(), _ntp, _rev, _remote);
    auto result = co_await dl.download_manifest(fib, &tmp);
    if (result.has_error()) {
        co_return std::make_pair(
          std::move(tmp), cloud_storage::download_result::failed);
    }

    // It's OK if the manifest is not found for a newly created topic. The
    // condition in if statement is not guaranteed to cover all cases for new
    // topics, so false positives may happen for this warn.
    if (
      result.value()
        == cloud_storage::find_partition_manifest_outcome::no_matching_manifest
      && _parent.high_watermark() != model::offset(0)
      && _parent.term() != model::term_id(1)) {
        vlog(
          _rtclog.warn,
          "Manifest for {} not found in S3, partition high_watermark: {}, "
          "partition term: {}",
          _ntp,
          _parent.high_watermark(),
          _parent.term());
        co_return std::make_pair(
          std::move(tmp), cloud_storage::download_result::notfound);
    }
    co_return std::make_pair(
      std::move(tmp), cloud_storage::download_result::success);
}

bool ntp_archiver::manifest_upload_required() const {
    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      == cluster::archival_metadata_stm::state_dirty::clean) {
        vlog(
          _rtclog.debug,
          "Manifest is clean{}, skipping upload",
          _projected_manifest_clean_at.has_value() ? " (projected)" : "");
        return false;
    }

    // Do not upload if interval has not elapsed.  The upload loop will call
    // us again periodically and we will eventually upload when we pass the
    // interval.  Make an exception if we are under local storage pressure,
    // and skip the interval wait in this case.
    if (
      !local_storage_pressure() && _manifest_upload_interval().has_value()
      && ss::lowres_clock::now() - _last_manifest_upload_time
           < _manifest_upload_interval().value()) {
        return false;
    }
    return true;
}

/**
 * Partition manifests are written somewhat lazily with respect to segment
 * uploads.  They are only uploaded if the stm is marked dirty by a write since
 * the last uploaded version, and if the time elapsed since last upload is
 * greater than the manifest upload interval.
 *
 * There are three cases for uploading the manifest:
 *
 * ### 1. High throughput partition
 *
 * If the stm is dirty while we are uploading segments, we may upload
 * the manifest concurrently with segment uploads, and mark_clean the stm
 * in the same batch as we add the uploaded segments to the stm, but the clean
 * offset is the offset _before_ this round of uploads.  The stm is left in a
 * dirty state, but that is okay: we will soon do more rounds of segment
 * uploads, and after manifest_upload_interval has elapsed, we will inline a
 * manifest upload in another round of segment uploads.
 *
 * In those mode we do zero extra stm I/Os for the marking the manifest clean,
 * and zero sequential waits for manifest uploads between segment uploads.
 *
 * ### 2. Low throughput partition
 *
 * If the stm is clean while we are uploading segments, then we may
 * upload the manifest sequentially _after_ we are done with segment uploads.
 * The _projected_manifest_clean_at is set to the offset reflected in the
 * uploaded manifest, but we do not write a mark_clean to the stm yet, to
 * avoid generating an additional write to the raft log.  We are in a
 * "projected clean" state where we will not do more manifest uploads
 * ourselves, but if we crashed or did an unsafe leader transfer, then
 * on restart the stm would look dirty and the manifest would be re-uploaded.
 *
 * In this mode we incur some sequential delay from uploading the manifest
 * sequentially with respect to segment uploads, but that is okay because
 * the upload loop is not saturated (that would hit case 1 above).  We only
 * rarely write extra mark_clean batches to the stm, in the case of a graceful
 * leadership transfer.
 *
 * ### 3. Fallback
 *
 * In cases where we have not run through a happy path (e.g. I/O errors,
 * unclean restarts, unclean leadership transfers), if the stm is dirty
 * then we will do an upload + mark_clean in the main upload loop, even
 * if we did not upload any segments.
 */
ss::future<bool> ntp_archiver::maybe_upload_manifest(const char* upload_ctx) {
    if (manifest_upload_required()) {
        auto result = co_await upload_manifest(upload_ctx);
        co_return result == cloud_storage::upload_result::success;
    }
    co_return false;
}

ss::future<> ntp_archiver::maybe_flush_manifest_clean_offset() {
    if (
      local_storage_pressure()
      || (_projected_manifest_clean_at.has_value() && ss::lowres_clock::now() - _last_marked_clean_time > _manifest_upload_interval())) {
        co_return co_await flush_manifest_clean_offset();
    }
}

ss::future<> ntp_archiver::flush_manifest_clean_offset() {
    if (!_projected_manifest_clean_at.has_value()) {
        co_return;
    }

    auto clean_offset = _projected_manifest_clean_at.value();
    auto deadline = ss::lowres_clock::now()
                    + config::shard_local_cfg()
                        .cloud_storage_metadata_sync_timeout_ms.value();
    auto errc = co_await _parent.archival_meta_stm()->mark_clean(
      deadline, clean_offset, _as);
    if (errc == raft::errc::shutting_down) {
        throw ss::abort_requested_exception();
    } else if (errc) {
        vlog(
          _rtclog.warn,
          "Failed to replicate clean message for "
          "archival_metadata_stm: {}",
          errc.message());
    } else {
        vlog(
          _rtclog.trace,
          "Marked archival_metadata_stm clean at offset {}",
          clean_offset);
        _projected_manifest_clean_at.reset();
        _last_marked_clean_time = ss::lowres_clock::now();
    }
}

ss::future<cloud_storage::upload_result> ntp_archiver::upload_manifest(
  const char* upload_ctx,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    if (!_feature_table.local().is_active(
          features::feature::cloud_storage_manifest_format_v2)) {
        vlog(
          archival_log.info,
          "[{}] Skipping manifest upload until all nodes in the cluster have "
          "been "
          "upgraded.",
          upload_ctx);

        co_return cloud_storage::upload_result::cancelled;
    }

    auto guard = _gate.hold();
    auto rtc = source_rtc.value_or(std::ref(_rtcnode));
    retry_chain_node fib(
      _conf->manifest_upload_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &rtc.get());
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    auto upload_insync_offset = manifest().get_insync_offset();

    auto path = manifest().get_manifest_path(remote_path_provider());
    vlog(
      _rtclog.debug,
      "[{}] Uploading partition manifest, insync_offset={}, path={}",
      upload_ctx,
      upload_insync_offset,
      path());

    auto result = co_await _remote.upload_manifest(
      get_bucket_name(), manifest(), path, fib);

    // now that manifest() is updated in cloud, updated the
    // compacted_away_cloud_bytes metric
    _probe.value().compacted_replaced_bytes(
      _parent.archival_meta_stm()->get_compacted_replaced_bytes());

    if (result == cloud_storage::upload_result::success) {
        _last_manifest_upload_time = ss::lowres_clock::now();
        _projected_manifest_clean_at = upload_insync_offset;
    } else {
        // It is not necessary to retry: we are called from within the main
        // upload_until_term_change loop, and will get another chance to
        // upload the manifest eventually from there.
        vlog(
          _rtclog.warn,
          "[{}] Failed to upload partition manifest at insync_offset={}: {}",
          upload_ctx,
          result,
          upload_insync_offset);
    }

    co_return result;
}

std::optional<ss::sstring> ntp_archiver::upload_should_abort() const {
    if (unlikely(lost_leadership())) {
        return fmt::format(
          "lost leadership or term changed during upload, "
          "current leadership status: {}, "
          "current term: {}, "
          "start term: {}",
          _parent.is_leader(),
          _parent.term(),
          _start_term);
    } else {
        return std::nullopt;
    }
}

ss::future<chunked_vector<model::tx_range>>
ntp_archiver::get_aborted_transactions(
  model::offset start_offset, model::offset end_offset) {
    auto guard = _gate.hold();
    co_return co_await _parent.aborted_transactions(start_offset, end_offset);
}

ss::future<std::pair<std::optional<chunked_vector<model::tx_range>>, size_t>>
ntp_archiver::get_aborted_transactions(
  const segment_collector_stream& meta,
  const cloud_storage::segment_name& sname) {
    ss::log_level level{};
    std::exception_ptr ep{};
    std::optional<chunked_vector<model::tx_range>> tx_ranges{};
    size_t tx_size{0};
    if (!meta.is_compacted) {
        try {
            tx_ranges = co_await get_aborted_transactions(
              meta.start_offset, meta.end_offset);
            tx_size = tx_ranges.value().size();
        } catch (...) {
            ep = std::current_exception();
            level = ssx::is_shutdown_exception(ep) ? ss::log_level::debug
                                                   : ss::log_level::warn;
        }
    }

    if (ep) {
        vlogl(
          _rtclog,
          level,
          "Failed to get aborted transactions for {}: {}",
          sname,
          ep);
        std::rethrow_exception(ep);
    }

    co_return std::make_pair(std::move(tx_ranges), tx_size);
}

ss::future<std::optional<ntp_archiver::make_segment_index_result>>
ntp_archiver::make_segment_index(
  model::offset base_rp_offset,
  model::timestamp base_timestamp,
  retry_chain_logger& ctxlog,
  std::string_view index_path,
  ss::input_stream<char> stream) {
    std::exception_ptr eptr;
    auto base_kafka_offset = [this, base_rp_offset, &eptr]() -> kafka::offset {
        try {
            return model::offset_cast(
              _parent.log()->from_log_offset(base_rp_offset));
        } catch (...) {
            eptr = std::current_exception();
            return kafka::offset{};
        }
    }();

    if (eptr) {
        co_await stream.close();
        std::rethrow_exception(eptr);
    }

    cloud_storage::offset_index ix{
      base_rp_offset,
      base_kafka_offset,
      0,
      cloud_storage::remote_segment_sampling_step_bytes,
      base_timestamp};

    vlog(ctxlog.debug, "creating remote segment index: {}", index_path);
    cloud_storage::segment_record_stats stats{};

    auto builder = cloud_storage::make_remote_segment_index_builder(
      _ntp,
      std::move(stream),
      ix,
      base_rp_offset - base_kafka_offset,
      cloud_storage::remote_segment_sampling_step_bytes,
      std::ref(stats));

    auto res = co_await builder->consume().finally(
      [&builder] { return builder->close(); });

    if (res.has_error()) {
        vlog(
          ctxlog.error,
          "failed to create remote segment index: {}, error: {}",
          index_path,
          res.error());
        co_return std::nullopt;
    }

    co_return make_segment_index_result{.index = std::move(ix), .stats = stats};
}

ss::future<std::optional<cloud_storage::upload_result>>
ntp_archiver::maybe_upload_aborted_tx(
  cloud_storage::remote_segment_path path,
  std::optional<chunked_vector<model::tx_range>> tx,
  retry_chain_node& parent_rtc) {
    retry_chain_node fib(&parent_rtc);
    if (tx.has_value() && !tx.value().empty()) {
        cloud_storage::tx_range_manifest manifest(path, std::move(tx).value());
        auto result = co_await _remote.upload_manifest(
          get_bucket_name(), manifest, manifest.get_manifest_path(), fib);
        co_return result;
    }
    co_return std::nullopt;
}

ss::future<> ntp_archiver::upload_index(
  ss::sstring path, cloud_storage::offset_index index) {
    retry_chain_node rtc{
      _conf->segment_upload_timeout(),
      _conf->upload_loop_initial_backoff(),
      &_rtcnode};
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    auto fut = co_await ss::coroutine::as_future(_remote.upload_index(
      _conf->bucket_name, cloud_storage_clients::object_key{path}, index, rtc));

    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(ctxlog.warn, "Index upload failed: {}", ex);
    }
}

ss::future<ntp_archiver_upload_result> ntp_archiver::upload_segment(
  segment_collector_stream strm,
  const cloud_storage::segment_meta& meta,
  std::optional<chunked_vector<model::tx_range>> tx_ranges) {
    retry_chain_node rtc(
      _conf->segment_upload_timeout(),
      _conf->upload_loop_initial_backoff(),
      &_rtcnode);
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    auto h = _gate.hold();
    auto path = manifest().generate_segment_path(meta, remote_path_provider());
    auto index_path = cloud_storage::generate_index_path(path).string();
    auto lazy_abort = lazy_abort_source{
      [this]() { return upload_should_abort(); }};
    auto stream = strm.create_input_stream();
    auto [upload_stream, indexing_stream] = input_stream_fanout<2>(
      std::move(stream),
      config::shard_local_cfg().storage_read_readahead_count());

    auto tx_upload_started = maybe_upload_aborted_tx(
      path, std::move(tx_ranges), rtc);

    auto make_index_started = make_segment_index(
      meta.base_offset,
      meta.base_timestamp,
      ctxlog,
      index_path,
      std::move(indexing_stream));

    // NOTE: input_stream_fanout returns non-optional input_streams. we move
    // the upload stream into an optional in order to detect whether
    // cloud_io::remote::upload_stream called the `get_stream` callback (below).
    // If not, we manually close the stream on the other side of the call to
    // upload_segment (also below).

    std::optional<ss::input_stream<char>> stream_state = std::move(
      upload_stream);

    auto get_stream = [&stream_state, &strm] {
        using provider_t = std::unique_ptr<stream_provider>;
        if (!stream_state.has_value()) {
            stream_state = strm.create_input_stream();
        }
        auto prov = std::make_unique<one_time_stream_wrapper>(
          std::move(stream_state).value());
        stream_state.reset();
        return ss::make_ready_future<provider_t>(std::move(prov));
    };

    // Upload segment in foreground and upload tx-manifest and build an
    // index in the background.
    auto upload_segment_ready = co_await ss::coroutine::as_future(
      _remote.upload_segment(
        get_bucket_name(), path, meta.size_bytes, get_stream, rtc, lazy_abort));

    // As noted above, check whether 'get_stream' was called. If not, close the
    // upload stream.
    if (stream_state.has_value()) {
        co_await stream_state->close();
    }

    // This future should be ready at the moment or will
    // be ready very soon. The index building is tied to
    // the segment upload through the fanout stream.
    auto make_index_ready = co_await ss::coroutine::as_future(
      std::move(make_index_started));
    auto tx_upload_ready = co_await ss::coroutine::as_future(
      std::move(tx_upload_started));

    struct error_state {
        ss::sstring source;
        std::exception_ptr e_ptr;
    };
    std::vector<error_state> e_ptr;
    auto log_exception =
      [&e_ptr](std::string_view source, const std::exception_ptr& e) {
          auto flat = flatten_exception(e);
          e_ptr.reserve(e_ptr.size() + flat.size());
          std::ranges::transform(
            flat,
            std::back_inserter(e_ptr),
            [source](const std::exception_ptr& e) -> error_state {
                return {
                  .source = ss::sstring{source.data(), source.size()},
                  .e_ptr = e};
            });
      };
    if (upload_segment_ready.failed()) {
        log_exception("Segment upload", upload_segment_ready.get_exception());
    }
    if (make_index_ready.failed()) {
        log_exception("Index construction", make_index_ready.get_exception());
    }
    if (tx_upload_ready.failed()) {
        log_exception("Tx-manifest upload", tx_upload_ready.get_exception());
    }

    size_t shutdown_error = 0;
    size_t other_failure = 0;
    if (!e_ptr.empty()) {
        for (auto [src, e] : e_ptr) {
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  ctxlog.debug,
                  "{} ({}) failed due to shutdown error",
                  src,
                  path);
                shutdown_error++;
            } else {
                vlog(ctxlog.warn, "{} ({}) upload failed: {}", src, path, e);
                other_failure++;
            }
        }
    }
    if (other_failure) {
        co_return ntp_archiver_upload_result(
          cloud_storage::upload_result::failed);
    }
    if (shutdown_error) {
        co_return ntp_archiver_upload_result(
          cloud_storage::upload_result::cancelled);
    }
    auto upload_result = upload_segment_ready.get();
    switch (upload_result) {
    case cloud_storage::upload_result::failed:
    case cloud_storage::upload_result::timedout:
        vlog(
          ctxlog.info,
          "Segment upload failed: {}, error: {}",
          path,
          upload_result);
        [[fallthrough]];
    case cloud_storage::upload_result::cancelled:
        co_return ntp_archiver_upload_result(upload_result);
    case cloud_storage::upload_result::success:
        break;
    }
    auto index_res = make_index_ready.get();
    if (!index_res.has_value()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Failed to construct segment for {}", path));
    }

    auto [index, index_stats] = std::move(index_res.value());

    // If we fail to upload the index but successfully upload the segment,
    // the read path will create the index on the fly while downloading the
    // segment, so it is okay to ignore the index upload failure, we still
    // want to advance the offsets because the segment did get uploaded.
    co_await upload_index(index_path, std::move(index));

    co_return ntp_archiver_upload_result(index_stats);

    // Note on segment locks, which were captured by
    // segment_collector_stream::create_input_stream:
    //
    // We've successfully uploaded the segment. Before we replicate an update
    // with the archival STM, drop any segment locks that may be held. It's
    // possible these locks are blocking other fibers from taking write locks,
    // which in turn, may prevent further read locks from being held.
    // Replicating and waiting on archival batches to be applied may require
    // taking read locks on these segments.
    //
    // Specifically, we want to avoid a series of events like:
    // 1. This fiber holds the uploaded segment's read lock
    // 2. Another fiber attempts to write lock the segment (e.g. during a
    //    segment roll), but can't. Instead, it prevents other read locks from
    //    being taken as it waits.
    // 3. This fiber attempts to replicate an archival batch, which
    //    subsequently waits for all prior ops to be applied, which may require
    //    consuming from this segment. In doing so, we attempt to read lock a
    //    locked segment.
    //
    // To avoid this, simply drop the locks here, now that we're done with
    // them. There's no concern that the underlying offsets will disappear
    // since the archival STM pins offsets until they are recorded in the
    // manifest.
}

auto ntp_archiver::do_schedule_single_upload_streaming(
  segment_collector_stream strm,
  model::term_id start_term,
  segment_upload_kind upload_kind) -> ss::future<scheduled_upload> {
    auto sname = segment_name_for_stream(strm, start_term);
    auto meta = convert_segment_meta(strm, _parent, _rev, start_term);
    auto [tx_ranges, tx_size] = co_await get_aborted_transactions(strm, sname);
    meta.metadata_size_hint = tx_size;

    vlog(
      _rtclog.debug,
      "Starting segment upload in the background, name: {}, meta: {}",
      sname,
      meta);

    auto background_upload = upload_segment(
      std::move(strm), meta, std::move(tx_ranges));

    // Happy path, we have found upload candidate and started uploading.
    // The upload is running in the background at the moment or will be
    // running soon.
    co_return scheduled_upload{
      .result = std::move(background_upload),
      .inclusive_last_offset = meta.committed_offset,
      .meta = meta,
      .name = sname, // TODO: check correctness
      .delta = meta.committed_offset - meta.base_offset,
      .stop = ss::stop_iteration::no,
      .upload_kind = upload_kind,
    };
}

ss::future<ntp_archiver::scheduled_upload>
ntp_archiver::schedule_single_upload(const upload_context& upload_ctx) {
    auto start_upload_offset = upload_ctx.start_offset;
    auto last_stable_offset = upload_ctx.end_offset_exclusive;

    auto log = _parent.log();

    segment_collector_stream_result candidate_result{};

    switch (upload_ctx.upload_kind) {
    case segment_upload_kind::non_compacted:
        candidate_result = co_await _policy.get_next_segment(
          start_upload_offset,
          last_stable_offset,
          _flush_uploads_offset,
          log,
          manifest(),
          _conf->segment_upload_timeout());
        break;
    case segment_upload_kind::compacted:
        candidate_result = co_await _policy.get_next_compacted_segment(
          start_upload_offset,
          log,
          manifest(),
          _conf->segment_upload_timeout());
        break;
    }

    co_return co_await ss::visit(
      candidate_result,
      [](std::monostate) -> ss::future<scheduled_upload> {
          vunreachable("Unexpected monostate in candidate stream result");
      },
      [this, &upload_ctx](segment_collector_stream& strm) {
          return do_schedule_single_upload_streaming(
            std::move(strm), upload_ctx.archiver_term, upload_ctx.upload_kind);
      },
      [this, &upload_ctx, start_upload_offset, last_stable_offset](
        skip_offset_range& skip_offsets) {
          vassert(
            upload_ctx.upload_kind == segment_upload_kind::compacted,
            "Invalid request to skip offset range for non-compacted upload: "
            "{}, start upload offset: {}, last stable offset: {}",
            skip_offsets,
            start_upload_offset,
            last_stable_offset);
          const auto log_level = log_level_for_error(skip_offsets.reason);
          vlogl(
            _rtclog,
            log_level,
            "Failed to make upload candidate, skipping offset range: {}",
            skip_offsets);
          return ss::make_ready_future<scheduled_upload>(scheduled_upload{
            .result = std::nullopt,
            .inclusive_last_offset = skip_offsets.end_offset,
            .meta = std::nullopt,
            .name = std::nullopt,
            .delta = std::nullopt,
            .stop = ss::stop_iteration::yes,
            .upload_kind = upload_ctx.upload_kind,
          });
      },
      [this, &upload_ctx](candidate_creation_error& error) {
          const auto log_level = log_level_for_error(error);
          vlogl(
            _rtclog, log_level, "failed to make upload candidate: {}", error);
          return ss::make_ready_future<scheduled_upload>(scheduled_upload{
            .result = std::nullopt,
            .inclusive_last_offset = {},
            .meta = std::nullopt,
            .name = std::nullopt,
            .delta = std::nullopt,
            .stop = ss::stop_iteration::yes,
            .upload_kind = upload_ctx.upload_kind,
          });
      });
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(model::offset max_offset_exclusive) {
    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains dirty_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if we haven't uploaded anything.
    //
    // When there are no segments but there is a non-zero 'last_offset', all
    // cloud segments have been removed for retention. In that case, we still
    // need to take into accout 'last_offset'.
    auto last_offset = manifest().get_last_offset();
    auto start_upload_offset = manifest().size() == 0
                                   && last_offset == model::offset(0)
                                 ? model::offset(0)
                                 : last_offset + model::offset(1);

    // If tiered storage was paused and gaps were allowed to be created
    // then we need to start from the first offset in the log.
    if (start_upload_offset < _parent.log()->offsets().start_offset) {
        vlog(
          _rtclog.warn,
          "Start upload offset {} is less than log start offset {}. Resetting "
          "start upload offset to log start offset.",
          start_upload_offset,
          _parent.log()->offsets().start_offset);
        start_upload_offset = _parent.log()->offsets().start_offset;
    }

    auto compacted_segments_upload_start = model::next_offset(
      manifest().get_last_uploaded_compacted_offset());

    std::vector<upload_context> params;

    params.push_back({
      .upload_kind = segment_upload_kind::non_compacted,
      .start_offset = start_upload_offset,
      .end_offset_exclusive = max_offset_exclusive,
      .allow_reuploads = allow_reuploads_t::no,
      .archiver_term = _start_term,
    });

    if (
      config::shard_local_cfg().cloud_storage_enable_compacted_topic_reupload()
      && _parent.get_ntp_config().is_locally_compacted()
      && compacted_segments_upload_start < start_upload_offset) {
        params.push_back({
          .upload_kind = segment_upload_kind::compacted,
          .start_offset = compacted_segments_upload_start,
          .end_offset_exclusive = model::offset::max(),
          .allow_reuploads = allow_reuploads_t::yes,
          .archiver_term = _start_term,
        });
    }

    co_return co_await schedule_uploads(std::move(params));
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(std::vector<upload_context> loop_contexts) {
    std::vector<scheduled_upload> scheduled_uploads;
    auto uploads_remaining = _concurrency;
    for (auto& ctx : loop_contexts) {
        if (uploads_remaining <= 0) {
            vlog(
              _rtclog.info,
              "no more upload slots remaining, skipping upload kind: {}, start "
              "offset: {}, last offset: {}, uploads remaining: {}",
              ctx.upload_kind,
              ctx.start_offset,
              ctx.end_offset_exclusive,
              uploads_remaining);
            break;
        }

        vlog(
          _rtclog.debug,
          "scheduling uploads, start offset: {}, last offset: {}, upload kind: "
          "{}, uploads remaining: {}",
          ctx.start_offset,
          ctx.end_offset_exclusive,
          ctx.upload_kind,
          uploads_remaining);

        // this metric is only relevant for non compacted uploads.
        if (ctx.upload_kind == segment_upload_kind::non_compacted) {
            _probe.value().upload_lag(
              ctx.end_offset_exclusive - ctx.start_offset);
        }

        std::exception_ptr ep;
        try {
            while (uploads_remaining > 0 && may_begin_uploads()) {
                auto scheduled = co_await schedule_single_upload(ctx);
                ctx.start_offset = model::next_offset(
                  scheduled.inclusive_last_offset);
                scheduled_uploads.push_back(std::move(scheduled));
                const auto& latest_scheduled = scheduled_uploads.back();
                if (latest_scheduled.stop == ss::stop_iteration::yes) {
                    break;
                }
                // Decrement remaining upload count if the last call actually
                // scheduled an upload.
                if (latest_scheduled.result.has_value()) {
                    uploads_remaining -= 1;
                }
            }
        } catch (...) {
            ep = std::current_exception();
        }
        if (ep) {
            vlog(_rtclog.warn, "Failed to schedule upload: {}", ep);
            std::vector<ss::future<>> inflight_uploads;
            for (auto& scheduled : scheduled_uploads) {
                if (scheduled.result.has_value()) {
                    inflight_uploads.emplace_back(
                      std::move(scheduled.result.value()).discard_result());
                }
            }
            auto futs = co_await ss::when_all(
              inflight_uploads.begin(), inflight_uploads.end());
            for (auto& f : futs) {
                if (f.failed()) {
                    auto ex = f.get_exception();
                    vlog(_rtclog.warn, "Upload failed: {}", ex);
                }
            }
            std::rethrow_exception(ep);
        }

        auto upload_segments_count = std::count_if(
          scheduled_uploads.begin(),
          scheduled_uploads.end(),
          [](const auto& upload) { return upload.result.has_value(); });
        vlog(
          _rtclog.debug,
          "scheduled {} uploads for upload kind: {}, uploads remaining: "
          "{}",
          upload_segments_count,
          ctx.upload_kind,
          uploads_remaining);
    }

    co_return scheduled_uploads;
}

ss::future<ntp_archiver::wait_uploads_complete_result>
ntp_archiver::wait_uploads_complete(
  std::vector<scheduled_upload> scheduled,
  segment_upload_kind segment_kind,
  bool inline_manifest) {
    wait_uploads_complete_result result{
      .inline_manifest = inline_manifest,
    };

    std::vector<ss::future<ntp_archiver_upload_result>> flist;
    std::vector<size_t> ixupload;
    for (size_t ix = 0; ix < scheduled.size(); ix++) {
        if (scheduled[ix].result) {
            flist.emplace_back(std::move(*scheduled[ix].result));
            ixupload.push_back(ix);
        }
    }

    if (flist.empty()) {
        vlog(
          _rtclog.debug,
          "no uploads started for segment upload kind: {}, returning",
          segment_kind);
        // The result is empty at this point
        co_return result;
    }

    // We may upload manifest in parallel with segments when using time-based
    // (interval) manifest uploads.  If we aren't using an interval, then the
    // manifest will always be immediately updated after segment uploads, so
    // there is no point doing it in parallel as well.
    bool upload_manifest_in_parallel
      = inline_manifest && _manifest_upload_interval().has_value();

    if (upload_manifest_in_parallel) {
        // Munge the output of maybe_upload_manifest into an upload result,
        // so that we can conveniently await it along with our segment
        // uploads.  The actual result is reflected in
        // _projected_manifest_clean_at if something was uploaded.
        flist.push_back(
          maybe_upload_manifest(concurrent_with_segs_ctx_label).then([](bool) {
              return ntp_archiver_upload_result{
                cloud_storage::upload_result::success};
          }));
    }

    auto segment_results = co_await ss::when_all_succeed(
      begin(flist), end(flist));

    if (upload_manifest_in_parallel) {
        // Drop the upload_result from manifest upload, we do not want to
        // count it in the subsequent success/failure counts.
        // The actual progress of the manifest uploads is tracked inside the
        // 'maybe_upload_manifest' method. The method updates projected clean
        // offset upon success. Later this value is used to replicate
        // 'mark_clean' command so we don't need to propagate the actual error
        // code.
        segment_results.pop_back();
    }

    if (!can_update_archival_metadata()) {
        // We exit early even if we have successfully uploaded some segments to
        // avoid interfering with an archiver that could have started on another
        // node.
        co_return wait_uploads_complete_result{};
    }

    absl::flat_hash_map<cloud_storage::upload_result, size_t> upload_results;
    for (const auto& result : segment_results) {
        ++upload_results[result.result()];
    }

    result.num_succeeded
      = upload_results[cloud_storage::upload_result::success];
    result.num_cancelled
      = upload_results[cloud_storage::upload_result::cancelled];
    result.num_failed = segment_results.size()
                        - (result.num_succeeded + result.num_cancelled);

    result.checks_disabled
      = config::shard_local_cfg()
          .cloud_storage_disable_upload_consistency_checks.value();
    auto skip_metadata_check = [this] {
        // Special handling of the situation when the gap was created
        // while the archiver was paused with 'redpanda.remote.allowgaps'
        // set to 'true'.
        // If the property is set to true and the local start offset
        // doesn't match the last uploaded offset we should skip the
        // check.
        auto manifest_last = manifest().get_last_offset();
        auto local_first = _parent.raft_start_offset();
        model::offset manifest_next = model::next_offset(manifest_last);
        bool gaps_allowed
          = _parent.log()->config().is_remote_allow_gaps_enabled();
        return gaps_allowed && !manifest().empty()
               && manifest_next < local_first;
    }();
    if (!skip_metadata_check && !result.checks_disabled) {
        // With read-write-fence it's guaranteed that the concurrent
        // updates are not a problem. But we still need this check
        // to prevent certain bugs from corrupting the cloud storage
        // metadata.
        // Overall, we're checking that the update makes sense here
        // (that the new segment lines up with the previous one). Then
        // we're checking that the segment actually matches its
        // metadata. And then we're replicating the metadata with the
        // fence that guarantees that no updates are made to the STM
        // state interim.
        // In other words, we're basing the decision to start an upload
        // on the precondition. Then we're validating the actual uploads
        // against this precondition and then we're discarding the
        // changes to the STM state if the precondition is no longer
        // valid.
        std::vector<cloud_storage::segment_meta> meta;
        for (size_t i = 0; i < segment_results.size(); i++) {
            meta.push_back(scheduled[ixupload[i]].meta.value());
        }
        size_t num_accepted = manifest().safe_segment_meta_to_add(
          std::move(meta));
        if (num_accepted < segment_results.size()) {
            vlog(
              _rtclog.warn,
              "Metadata inconsistency detected, {} segments uploaded but only "
              "{} can be added",
              segment_results.size(),
              num_accepted);
            _probe.value().gap_detected(
              model::offset(
                static_cast<int64_t>(segment_results.size() - num_accepted)));
        }
        vassert(
          num_accepted <= segment_results.size(),
          "Accepted {} segments but only {} segments are uploaded",
          num_accepted,
          segment_results.size());
        segment_results.resize(num_accepted);
    }
    for (size_t i = 0; i < segment_results.size(); i++) {
        if (
          segment_results[i].result()
          != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];
        if (!result.checks_disabled && segment_results[i].has_record_stats()) {
            // Validate metadata by comparing it to the segment stats
            // generated during index building process. The stats contains
            // "ground truth" about the uploaded segment because the code that
            // generates it was "looking" at every record batch before it was
            // sent out.
            //
            // By doing this incrementally we can build consistent log metadata
            // because every such check is based on previous state that was
            // also validated using the same procedure.
            auto stats = segment_results[i].record_stats();
            if (
              upload.upload_kind == segment_upload_kind::non_compacted
              && upload.meta.has_value()) {
                if (!segment_meta_matches_stats(
                      *upload.meta,
                      stats,
                      _rtclog,
                      _parent.get_ntp_config()
                        .is_remote_allow_gaps_enabled())) {
                    break;
                }
            }
        }

        if (segment_kind == segment_upload_kind::non_compacted) {
            _probe.value().uploaded(*upload.delta);
            _probe.value().uploaded_bytes(upload.meta->size_bytes);

            model::offset expected_base_offset;
            if (manifest().get_last_offset() < model::offset{0}) {
                expected_base_offset = model::offset{0};
            } else {
                expected_base_offset = manifest().get_last_offset()
                                       + model::offset{1};
            }
        }

        result.meta.push_back(*upload.meta);
    }
    if (result.num_succeeded > result.meta.size()) {
        vlog(
          _rtclog.info,
          "Some segments were discarded due to metadata consistency violation: "
          "{} uploaded vs {} accepted",
          result.num_succeeded,
          result.meta.size());
        auto num_discarded = result.num_succeeded - result.meta.size();
        result.num_succeeded -= num_discarded;
        result.num_failed += num_discarded;
    }
    co_return result;
}

/// Replicate archival metadata
ss::future<ntp_archiver::upload_group_result>
ntp_archiver::replicate_archival_metadata(
  archival_stm_fence fence,
  std::vector<wait_uploads_complete_result> finished_uploads) {
    upload_group_result total{};
    std::vector<cloud_storage::segment_meta> meta;
    bool checks_disabled = true;
    bool inline_manifest = false;
    for (const auto& it : finished_uploads) {
        total.num_cancelled += it.num_cancelled;
        total.num_failed += it.num_failed;
        total.num_succeeded += it.num_succeeded;
        for (const auto& s : it.meta) {
            meta.emplace_back(s);
        }
        checks_disabled = checks_disabled && it.checks_disabled;
        inline_manifest = inline_manifest || it.inline_manifest;
    }

    // Remember if we started with a clean STM: this will be used to decide
    // whether to maybe do an extra flush of manifest after upload, to get back
    // into a clean state.
    auto stm_was_clean = _parent.archival_meta_stm()->get_dirty(
                           _projected_manifest_clean_at)
                         == cluster::archival_metadata_stm::state_dirty::clean;

    if (meta.empty()) {
        vlog(_rtclog.debug, "No upload metadata collected, returning early");
        co_return total;
    }

    if (total.num_succeeded != 0) {
        vassert(
          _parent.archival_meta_stm(),
          "Archival metadata STM is not created for {} archiver",
          _ntp.path());

        auto deadline = ss::lowres_clock::now()
                        + _conf->manifest_upload_timeout();

        std::optional<model::offset> manifest_clean_offset;
        if (
          _projected_manifest_clean_at
          > _parent.archival_meta_stm()->get_last_clean_at()) {
            // If we have a projected clean offset, take this opportunity to
            // persist that to the stm.  This is equivalent to what
            // flush_manifest_clean_offset does, but we're doing it
            // inline with our segment-adding batch.
            manifest_clean_offset = _projected_manifest_clean_at;
        }
        auto highest_producer_id
          = _feature_table.local().is_active(
              features::feature::cloud_metadata_cluster_recovery)
              ? _parent.highest_producer_id()
              : model::producer_id{};

        auto is_validated = checks_disabled ? cluster::segment_validated::no
                                            : cluster::segment_validated::yes;
        cluster::emit_read_write_fence rw_fence = std::nullopt;
        if (fence.emit_rw_fence_cmd) {
            // The fence should be added first because it can only
            // affect commands which are following it in the same record
            // batch.
            vlog(
              archival_log.debug,
              "add_segments, read-write fence: {}, manifest "
              "last "
              "applied offset: {}, manifest in-sync offset: {}",
              fence.read_write_fence,
              _parent.archival_meta_stm()->manifest().get_applied_offset(),
              _parent.archival_meta_stm()->get_insync_offset());
            rw_fence = fence.read_write_fence;
        }
        auto error = co_await _parent.archival_meta_stm()->add_segments(
          meta,
          manifest_clean_offset,
          highest_producer_id,
          deadline,
          _as,
          is_validated,
          rw_fence);

        if (
          error != cluster::errc::success
          && error != cluster::errc::not_leader) {
            vlog(
              _rtclog.warn,
              "archival metadata STM update failed: {}",
              error.message());
        } else {
            // We have flushed projected clean offset if it was set
            if (_projected_manifest_clean_at.has_value()) {
                _last_marked_clean_time = ss::lowres_clock::now();
            }
            _projected_manifest_clean_at.reset();
        }

        vlog(
          _rtclog.debug,
          "successfully uploaded {} segments (failed {} uploads)",
          total.num_succeeded,
          total.num_failed);

        if (
          inline_manifest
          && (stm_was_clean || !_manifest_upload_interval().has_value())) {
            // This is the path for uploading manifests for infrequent*
            // segment uploads: we transitioned from clean to dirty, and the
            // manifest upload interval has expired.
            //
            // * infrequent means we're uploading a segment less often than the
            //   manifest upload interval, so can afford to upload manifest
            //   immediately after each segment upload.
            co_await maybe_upload_manifest(post_add_segs_ctx_label);
        }
    }

    co_return total;
}

ss::future<ntp_archiver::upload_group_result> ntp_archiver::wait_uploads(
  archival_stm_fence fence,
  std::vector<scheduled_upload> scheduled,
  segment_upload_kind segment_kind,
  bool inline_manifest) {
    auto wait_result = co_await wait_uploads_complete(
      std::move(scheduled), segment_kind, inline_manifest);
    co_return co_await replicate_archival_metadata(
      fence, {std::move(wait_result)});
}

ss::future<ntp_archiver::batch_result> ntp_archiver::wait_all_scheduled_uploads(
  archival_stm_fence fence,
  std::vector<ntp_archiver::scheduled_upload> scheduled) {
    // Split the set of scheduled uploads into compacted and non compacted
    // uploads, and then wait for them separately. They can also be waited on
    // together, but in the wait function we stop on the first failed upload.
    // If we wait on them together, a failed upload during compacted schedule
    // will stop any subsequent non-compacted uploads from being processed, and
    // as a result the upload offset will not be advanced for non-compacted
    // uploads.
    // Because the set of uploads advance two different offsets, this is
    // not ideal. A failed compacted segment upload should only stop the
    // compacted offset advance, so we split and wait on them separately.
    std::vector<ntp_archiver::scheduled_upload> non_compacted_uploads;
    std::vector<ntp_archiver::scheduled_upload> compacted_uploads;
    non_compacted_uploads.reserve(scheduled.size());
    compacted_uploads.reserve(scheduled.size());

    std::partition_copy(
      std::make_move_iterator(scheduled.begin()),
      std::make_move_iterator(scheduled.end()),
      std::back_inserter(non_compacted_uploads),
      std::back_inserter(compacted_uploads),
      [](const scheduled_upload& s) {
          return s.upload_kind == segment_upload_kind::non_compacted;
      });

    // Inline manifest upload in regular non-compacted uploads
    // if any were scheduled.
    bool inline_manifest_in_non_compacted_uploads = false;
    for (const auto& i : non_compacted_uploads) {
        if (i.result) {
            inline_manifest_in_non_compacted_uploads = true;
            break;
        }
    }

    auto [non_compacted_result, compacted_result]
      = co_await ss::when_all_succeed(
        wait_uploads_complete(
          std::move(non_compacted_uploads),
          segment_upload_kind::non_compacted,
          inline_manifest_in_non_compacted_uploads),
        wait_uploads_complete(
          std::move(compacted_uploads),
          segment_upload_kind::compacted,
          !inline_manifest_in_non_compacted_uploads));

    // Replicate the metadata
    auto final_result = co_await replicate_archival_metadata(
      fence, {non_compacted_result, compacted_result});

    if (final_result.num_succeeded > 0) {
        _last_segment_upload_time = ss::lowres_clock::now();
    }
    vlog(
      _rtclog.trace,
      "Segment uploads complete: {} successful uploads",
      final_result.num_succeeded);

    co_return batch_result{
      .non_compacted_upload_result = {
        .num_succeeded = non_compacted_result.num_succeeded,
        .num_failed = non_compacted_result.num_failed,
        .num_cancelled = non_compacted_result.num_cancelled,
      },
      .compacted_upload_result = {
        .num_succeeded = compacted_result.num_succeeded,
        .num_failed = compacted_result.num_failed,
        .num_cancelled = compacted_result.num_cancelled,
      },
      };
}

model::offset ntp_archiver::max_uploadable_offset_exclusive() const {
    // We impose an additional (LSO) constraint on the uploadable offset to
    // as we need to have a complete index of aborted transactions if any
    // before we can upload a segment.
    return std::min(
      _parent.last_stable_offset(),
      model::next_offset(_parent.committed_offset()));
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  archival_stm_fence fence,
  std::optional<model::offset> unsafe_max_offset_override_exclusive) {
    auto max_offset_exclusive = unsafe_max_offset_override_exclusive
                                  ? *unsafe_max_offset_override_exclusive
                                  : max_uploadable_offset_exclusive();
    vlog(
      _rtclog.debug,
      "Uploading next candidates called for {} with max_offset_exclusive={}",
      _ntp,
      max_offset_exclusive);
    ss::gate::holder holder(_gate);
    try {
        auto units = co_await _mutex.get_units(_as);
        auto scheduled_uploads = co_await schedule_uploads(
          max_offset_exclusive);
        co_return co_await wait_all_scheduled_uploads(
          fence, std::move(scheduled_uploads));
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::abort_requested_exception&) {
    }
    co_return batch_result{
      .non_compacted_upload_result = {}, .compacted_upload_result = {}};
}

uint64_t ntp_archiver::estimate_backlog_size() {
    auto last_offset = manifest().get_last_offset();
    auto log = _parent.log();
    uint64_t total_size = std::accumulate(
      std::begin(log->segments()),
      std::end(log->segments()),
      0UL,
      [last_offset](
        uint64_t acc, const ss::lw_shared_ptr<storage::segment>& s) {
          if (s->offsets().get_dirty_offset() > last_offset) {
              return acc + s->size_bytes();
          }
          return acc;
      });
    // Note: we can safely ignore the fact that the last segment is not uploaded
    // before it's sealed because the size of the individual segment is small
    // compared to the capacity of the data volume.
    return total_size;
}

ss::future<std::optional<cloud_storage::partition_manifest>>
ntp_archiver::maybe_truncate_manifest() {
    retry_chain_node rtc(_as);
    ss::gate::holder gh(_gate);
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    vlog(ctxlog.info, "archival metadata cleanup started");
    model::offset adjusted_start_offset = model::offset::min();
    const auto& m = manifest();
    for (const auto& meta : m) {
        retry_chain_node fib(
          _conf->manifest_upload_timeout(),
          _conf->upload_loop_initial_backoff(),
          &rtc);
        auto spath = m.generate_segment_path(meta, remote_path_provider());
        auto result = co_await _remote.segment_exists(
          get_bucket_name(), spath, fib);
        if (result == cloud_storage::download_result::notfound) {
            vlog(
              ctxlog.info,
              "archival metadata cleanup, found segment missing from the "
              "bucket: {}",
              spath);
            adjusted_start_offset = meta.committed_offset + model::offset(1);
        } else {
            break;
        }
    }
    std::optional<cloud_storage::partition_manifest> result;
    if (
      adjusted_start_offset != model::offset::min()
      && _parent.archival_meta_stm()) {
        vlog(
          ctxlog.info,
          "archival metadata cleanup, some segments will be removed from the "
          "manifest, start offset before cleanup: {}",
          manifest().get_start_offset());
        auto error = co_await _parent.archival_meta_stm()->truncate(
          adjusted_start_offset,
          ss::lowres_clock::now() + _conf->manifest_upload_timeout(),
          _as);
        if (error != cluster::errc::success) {
            vlog(
              ctxlog.warn,
              "archival metadata STM update failed: {}",
              error.message());
            throw std::system_error(error);
        } else {
            vlog(
              ctxlog.debug,
              "archival metadata STM update passed, re-uploading manifest");

            // The manifest upload will be retried after some time because the
            // STM will have the 'dirty' flag set. `upload_manifest` will log an
            // error so it won't be invisible.
            std::ignore = co_await upload_manifest(sync_local_state_ctx_label);
        }
        vlog(
          ctxlog.info,
          "archival metadata cleanup completed, start offset after cleanup: {}",
          manifest().get_start_offset());
    } else {
        // Nothing to cleanup, return empty manifest
        result = cloud_storage::partition_manifest(_ntp, _rev);
        vlog(
          ctxlog.info,
          "archival metadata cleanup completed, nothing to clean up");
    }
    co_return result;
}

std::ostream& operator<<(std::ostream& os, segment_upload_kind upload_kind) {
    switch (upload_kind) {
    case segment_upload_kind::non_compacted:
        fmt::print(os, "non-compacted");
        break;
    case segment_upload_kind::compacted:
        fmt::print(os, "compacted");
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, flush_response fr) {
    switch (fr) {
    case flush_response::accepted:
        fmt::print(os, "accepted");
        break;
    case flush_response::rejected:
        fmt::print(os, "rejected");
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, flush_result fr) {
    fmt::print(os, "response: {}, offset: {}", fr.response, fr.offset);
    return os;
}

std::ostream& operator<<(std::ostream& os, wait_result fr) {
    switch (fr) {
    case wait_result::not_in_progress:
        return os << "not in progress";
    case wait_result::complete:
        return os << "complete";
    case wait_result::lost_leadership:
        return os << "lost leadership";
    case wait_result::failed:
        return os << "failed";
    }
    return os;
}

ss::future<ntp_archiver::housekeeping_result> ntp_archiver::housekeeping() {
    auto result = housekeeping_result::complete;
    try {
        if (may_begin_uploads()) {
            // Acquire mutex to prevent concurrency between
            // external housekeeping jobs from upload_housekeeping_service
            // and retention/GC
            auto units = co_await _mutex.get_units(_as);
            if (stm_retention_needed()) {
                co_await apply_retention();
                co_await garbage_collect();
            } else {
                co_await apply_archive_retention();
                result = co_await garbage_collect_archive();
                co_await garbage_collect();
            }
            co_await apply_spillover();
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::broken_semaphore&) {
    } catch (const ss::semaphore_timed_out&) {
        // Shutdown-type exceptions are thrown, to promptly drop out
        // of the upload loop.
        throw;
    } catch (const std::exception& e) {
        // Unexpected exceptions are logged, and suppressed: we do not
        // want to stop who upload loop because of issues in housekeeping
        result = housekeeping_result::error;
        vlog(_rtclog.warn, "Error occurred during housekeeping: {}", e.what());
    }

    co_return result;
}

ss::future<> ntp_archiver::apply_archive_retention() {
    if (!may_begin_uploads()) {
        co_return;
    }

    const auto& ntp_conf = _parent.get_ntp_config();
    if (!ntp_conf.is_remotely_collectable()) {
        vlog(_rtclog.trace, "NTP is not collectable");
        co_return;
    }

    auto fence = emit_rw_fence();

    std::optional<size_t> retention_bytes = ntp_conf.retention_bytes();
    std::optional<std::chrono::milliseconds> retention_ms
      = ntp_conf.retention_duration();

    auto pinned_offset
      = _parent.raft()->log()->stm_hookset()->lowest_pinned_data_offset();
    auto res = co_await _manifest_view->compute_retention(
      retention_bytes, retention_ms, pinned_offset);

    if (res.has_error()) {
        if (res.error() == cloud_storage::error_outcome::shutting_down) {
            vlog(
              _rtclog.debug,
              "Search for archive retention point failed as Redpanda is "
              "shutting down");
            co_return;
        } else {
            vlog(
              _rtclog.error,
              "Failed to compute archive retention: {}",
              res.error());
            throw std::system_error(res.error());
        }
    }

    if (
      res.value().offset == model::offset{}
      || res.value().offset
           <= _manifest_view->stm_manifest().get_archive_start_offset()) {
        co_return;
    }

    // Replicate metadata
    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto deadline = ss::lowres_clock::now() + sync_timeout;

    auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
    if (fence.emit_rw_fence_cmd) {
        vlog(
          _rtclog.debug,
          "truncate_archive_init, read-write fence: {}",
          fence.read_write_fence);
        batch.read_write_fence(fence.read_write_fence);
    }
    batch.truncate_archive_init(res.value().offset, res.value().delta);
    auto error = co_await batch.replicate();

    if (error != cluster::errc::success) {
        vlog(
          _rtclog.warn,
          "Failed to replicate archive truncation command: {}",
          error.message());

        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Failed to apply archive retention: {}", error));
    } else {
        vlog(
          _rtclog.info,
          "Archive truncated to offset {} (delta: {})",
          res.value().offset,
          res.value().delta);
    }
}

ss::future<ntp_archiver::housekeeping_result>
ntp_archiver::garbage_collect_archive() {
    if (!may_begin_uploads()) {
        co_return housekeeping_result::complete;
    }
    auto fence = emit_rw_fence();
    auto backlog = co_await _manifest_view->get_retention_backlog();
    if (backlog.has_failure()) {
        if (backlog.error() == cloud_storage::error_outcome::shutting_down) {
            vlog(
              _rtclog.debug,
              "Skipping archive GC as Redpanda is shutting down");
            co_return housekeeping_result::complete;
        }

        vlog(
          _rtclog.error,
          "Failed to create GC backlog for the archive: {}",
          backlog.error());
        throw std::system_error(backlog.error());
    }

    std::deque<cloud_storage_clients::object_key> objects_to_remove;
    std::deque<cloud_storage_clients::object_key> manifests_to_remove;

    const auto clean_offset = manifest().get_archive_clean_offset();
    const auto start_offset = manifest().get_archive_start_offset();
    const size_t max_segments = _gc_max_segments();

    vlog(
      _rtclog.info,
      "Garbage collecting archive segments in offset range [{}, {}) with batch "
      "limit {}",
      clean_offset,
      start_offset,
      max_segments);

    if (clean_offset == start_offset) {
        vlog(
          _rtclog.debug,
          "Garbage collection in the archive not required as clean offset "
          "equals the start offset ({})",
          clean_offset);
        co_return housekeeping_result::complete;
    } else if (clean_offset > start_offset) {
        vlog(
          _rtclog.error,
          "Garbage collection requested until offset {}, but start offset is "
          "at {}. Skipping garbage collection.",
          clean_offset,
          start_offset);
        co_return housekeeping_result::error;
    }

    model::offset new_clean_offset{clean_offset};
    // Value includes segments but doesn't include manifests
    size_t bytes_to_remove = 0;
    size_t segments_to_remove_count = 0;

    auto cursor = std::move(backlog.value());

    using eof = cloud_storage::async_manifest_view_cursor::eof;
    while (cursor->get_status()
           == cloud_storage::async_manifest_view_cursor_status::
             materialized_spillover) {
        auto stop = co_await cursor->with_manifest(
          [&](const cloud_storage::partition_manifest& manifest) {
              for (const auto& meta : manifest) {
                  if (meta.committed_offset < clean_offset) {
                      // The manifest is only removed if all segments are
                      // deleted. Because of that we may end up in a situation
                      // when some of the segments are deleted and the rest are
                      // not. The spillover manifest is never adjusted
                      //  and reuploaded after GC.
                      continue;
                  }
                  if (meta.committed_offset < start_offset) {
                      if (segments_to_remove_count >= max_segments) {
                          return true;
                      }
                      const auto path = manifest.generate_segment_path(
                        meta, remote_path_provider());
                      vlog(
                        _rtclog.info,
                        "Enqueuing spillover segment delete from cloud "
                        "storage: {}",
                        path);
                      objects_to_remove.emplace_back(path);
                      new_clean_offset = model::next_offset(
                        meta.committed_offset);
                      bytes_to_remove += meta.size_bytes;
                      ++segments_to_remove_count;
                      // Add index and tx-manifest
                      if (
                        meta.sname_format
                          == cloud_storage::segment_name_format::v3
                        && meta.metadata_size_hint != 0) {
                          objects_to_remove.emplace_back(
                            cloud_storage::generate_remote_tx_path(path));
                      }
                      objects_to_remove.emplace_back(
                        cloud_storage::generate_index_path(path));
                  } else {
                      // This indicates that we need to remove only some of the
                      // segments from the manifest. In this case the outer loop
                      // needs to stop and the current manifest shouldn't be
                      // marked for deletion.
                      return true;
                  }
              }
              return false;
          });

        if (stop) {
            break;
        }
        const auto path = cursor->manifest()->get_manifest_path(
          remote_path_provider());
        vlog(
          _rtclog.info,
          "Enqueuing spillover manifest delete from cloud "
          "storage: {}",
          path);
        manifests_to_remove.emplace_back(path);

        if (segments_to_remove_count >= max_segments) {
            break;
        }

        auto res = co_await cursor->next();
        if (res.has_failure()) {
            if (res.error() == cloud_storage::error_outcome::shutting_down) {
                vlog(
                  _rtclog.debug,
                  "Stopping archive GC as Redpanda is shutting down");
                co_return housekeeping_result::complete;
            }

            vlog(
              _rtclog.error,
              "Failed to load next spillover manifest: {}",
              res.error());
            break;
        } else if (res.value() == eof::yes) {
            // End of stream
            break;
        }
    }

    if (segments_to_remove_count >= max_segments) {
        vlog(
          _rtclog.trace,
          "Archive GC batch limit reached, initial clean offset {}, new clean "
          "offset {}, start offset {}, segments to remove {}",
          clean_offset,
          new_clean_offset,
          start_offset,
          segments_to_remove_count);
    }

    // Drop out if we have no work to do, avoid doing things like the following
    // manifest flushing unnecessarily. This is problematic since, we've already
    // checked that the clean offset is greater than the start offset.
    if (objects_to_remove.empty() && manifests_to_remove.empty()) {
        vlog(_rtclog.error, "Nothing to remove in archive GC");
        co_return housekeeping_result::error;
    }

    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      != cluster::archival_metadata_stm::state_dirty::clean) {
        auto result = co_await upload_manifest("pre-garbage-collect-archive");
        if (result != cloud_storage::upload_result::success) {
            co_return housekeeping_result::error;
        }
    }

    retry_chain_node fib(
      _conf->garbage_collect_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &_rtcnode);
    const auto delete_result = co_await _remote.delete_objects(
      get_bucket_name(), objects_to_remove, fib);
    const auto all_deletes_succeeded = delete_result
                                       == cloud_storage::upload_result::success;

    if (!all_deletes_succeeded) {
        vlog(
          _rtclog.info,
          "Failed to delete all selected segments from cloud storage. Will "
          "retry on the next housekeeping run.");
        co_return housekeeping_result::error;
    } else {
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "cleanup_archive, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.cleanup_archive(new_clean_offset, bytes_to_remove);
        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.info,
              "Failed to clean up metadata after garbage collection: {}",
              error);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to clean up metadata after archive GC: {}",
              error));
        } else {
            std::ignore = co_await _remote.delete_objects(
              get_bucket_name(), manifests_to_remove, fib);
        }

        _probe.value().segments_deleted(
          static_cast<int64_t>(segments_to_remove_count));
        vlog(
          _rtclog.info,
          "Archive GC deleted {} segments, clean offset {}, start offset {}",
          segments_to_remove_count,
          new_clean_offset,
          start_offset);

        co_return new_clean_offset < start_offset
          ? housekeeping_result::partial
          : housekeeping_result::complete;
    }
}

ss::future<> ntp_archiver::apply_spillover() {
    const auto manifest_size_limit
      = config::shard_local_cfg().cloud_storage_spillover_manifest_size.value();
    const auto manifest_max_segments
      = config::shard_local_cfg()
          .cloud_storage_spillover_manifest_max_segments.value();
    if (
      manifest_size_limit.has_value() == false
      && manifest_max_segments.has_value() == false) {
        co_return;
    }

    if (!may_begin_uploads()) {
        co_return;
    }
    archival_stm_fence fence = emit_rw_fence();
    const auto manifest_upload_timeout = _conf->manifest_upload_timeout();
    const auto manifest_upload_backoff = _conf->cloud_storage_initial_backoff();

    // Check the spillover invariant.
    // The start_offset of the manifest must be equal to the begin_offset of
    // the first segment in the manifest.
    if (
      auto so = manifest().get_start_offset();
      so.has_value() && !manifest().empty()) {
        auto fo = manifest().begin()->base_offset;
        if (fo != so.value()) {
            vlog(
              _rtclog.warn,
              "Spillover invariant violated: manifest start_offset {}, first "
              "segment base_offset {}",
              so.value(),
              fo);
            co_return;
        }
    }

    if (manifest_size_limit.has_value()) {
        vlog(
          _rtclog.debug,
          "Manifest size: {}, manifest size limit (x2): {}",
          manifest().segments_metadata_bytes(),
          manifest_size_limit.value() * 2);
    } else {
        vlog(
          _rtclog.debug,
          "Manifest size: {}, manifest number of segments limit (x2): {}",
          manifest().size(),
          manifest_max_segments.value() * 2);
    }
    auto stop_condition = [&] {
        if (manifest_size_limit.has_value()) {
            return manifest().segments_metadata_bytes()
                   < manifest_size_limit.value() * 2;
        }
        return manifest().size() < manifest_max_segments.value() * 2;
    };
    auto spillover_complete = [&](
                                const cloud_storage::spillover_manifest& tail) {
        // Don't allow empty spillover manifests even if the limit
        // is too low.
        if (manifest_size_limit.has_value()) {
            return tail.segments_metadata_bytes() >= manifest_size_limit.value()
                   && tail.size() > 0;
        }
        return tail.size() >= manifest_max_segments.value() && tail.size() > 0;
    };
    while (!stop_condition()) {
        auto tail = [&]() {
            cloud_storage::spillover_manifest tail(_ntp, _rev);
            for (const auto& meta : manifest()) {
                vlog(
                  _rtclog.trace,
                  "Adding segment {} to the spillover manifest that starts at "
                  "{}",
                  meta,
                  tail.get_start_offset().value_or(model::offset{}));
                tail.add(meta);
                // No performance impact since all writes here are
                // sequential.
                tail.flush_write_buffer();
                if (spillover_complete(tail)) {
                    break;
                }
            }
            return tail;
        }();
        vlog(
          _rtclog.info,
          "Preparing spillover: manifest has {} segments and {} bytes, "
          "spillover manifest num elements: {}, size: {} bytes, base: {}, "
          "last: {}",
          manifest().size(),
          manifest().segments_metadata_bytes(),
          tail.size(),
          tail.segments_metadata_bytes(),
          tail.get_start_offset().value_or(model::offset{}),
          tail.get_last_offset());

        const auto first = *tail.begin();
        const auto last = tail.last_segment();
        const auto spillover_meta = tail.make_manifest_metadata();
        vassert(last.has_value(), "Spillover manifest can't be empty");
        vlog(
          _rtclog.info,
          "First batch of the spillover manifest: {}, Last batch of the "
          "spillover manifest: {}, spillover metadata: {}",
          first,
          last,
          spillover_meta);

        retry_chain_node upload_rtc(
          manifest_upload_timeout, manifest_upload_backoff, &_rtcnode);
        const auto path = tail.get_manifest_path(remote_path_provider());
        auto res = co_await _remote.upload_manifest(
          get_bucket_name(), tail, path, upload_rtc);
        if (res != cloud_storage::upload_result::success) {
            vlog(_rtclog.error, "Failed to upload spillover manifest {}", res);
            co_return;
        }
        auto [str, len] = co_await tail.serialize();
        // Put manifest into cache to avoid roundtrip to the cloud storage
        auto reservation = co_await _cache.reserve_space(len, 1);
        co_await _cache.put(
          tail.get_manifest_path(remote_path_provider())(), str, reservation);

        // Spillover manifests were uploaded to S3
        // Replicate metadata
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "spillover, read-write fence: {}",
              fence.read_write_fence);
            batch.read_write_fence(fence.read_write_fence);
        }
        batch.spillover(spillover_meta);
        if (manifest().get_archive_start_offset() == model::offset{}) {
            vlog(
              _rtclog.debug,
              "Archive is empty, have to set start archive/clean offset: {}, "
              "and delta: {}",
              first.base_offset,
              first.delta_offset);
            // Enable archive if this is the first spillover manifest. In this
            // case we need to set initial values for
            // archive_start_offset/archive_clean_offset which will be advanced
            // by housekeeping further on.
            batch.truncate_archive_init(first.base_offset, first.delta_offset);
            batch.cleanup_archive(first.base_offset, 0);
        }
        auto error = co_await batch.replicate();
        if (error != cluster::errc::success) {
            vlog(
              _rtclog.warn,
              "Failed to replicate spillover command: {}",
              error.message());
            break;
        } else {
            vlog(
              _rtclog.info,
              "Uploaded spillover manifest: {}",
              tail.get_manifest_path(remote_path_provider()));
        }
        // Reset fence for the next iteration
        fence = emit_rw_fence();
    }
}

flush_result ntp_archiver::flush() {
    // Return early if we are not the leader, or if we are a read replica.
    if (!_parent.is_leader() || _parent.is_read_replica_mode_enabled()) {
        vlog(
          _rtclog.debug,
          "Flush request not accepted, node is not the leader for the "
          "partition, or the node is a read replica");
        return flush_result{
          .response = flush_response::rejected, .offset = std::nullopt};
    }

    _flush_uploads_offset = model::prev_offset(
      max_uploadable_offset_exclusive());
    _wakeup_event.set();
    vlog(
      _rtclog.debug,
      "Accepted flush, flush offset is {}",
      _flush_uploads_offset.value());
    return flush_result{
      .response = flush_response::accepted, .offset = _flush_uploads_offset};
}

ss::future<wait_result> ntp_archiver::wait(model::offset o) {
    ss::gate::holder holder(_gate);

    if (_parent.is_read_replica_mode_enabled()) {
        vlog(
          _rtclog.debug,
          "Cannot wait on a flush in ntp_archiver, node is read replica");
        co_return wait_result::failed;
    }

    // Currently we tie wait() to flush() state. If there is no flush in
    // progress, we return.
    if (!flush_in_progress()) {
        vlog(
          _rtclog.debug,
          "Cannot wait on a flush in ntp_archiver, as no flush in progress");
        co_return wait_result::not_in_progress;
    }

    // If o is outside bounds of _flush_uploads_offset, we indicate so.
    if (o > _flush_uploads_offset.value()) {
        vlog(
          _rtclog.debug,
          "Passed offset {} is outside bounds of current flush offset {}",
          o,
          _flush_uploads_offset.value());
        co_return wait_result::not_in_progress;
    }

    // Save the _start_term before entering wait loop, where we will suspend on
    // condition variable.
    auto wait_issued_term = _start_term;

    while (!uploaded_and_clean_past_offset(o)) {
        if (
          !_parent.is_leader() || wait_issued_term != _start_term
          || !flush_in_progress()) {
            vlog(
              _rtclog.debug,
              "Leadership was lost during flush operation in ntp_archiver");
            co_return wait_result::lost_leadership;
        }

        try {
            vlog(_rtclog.trace, "Waiting on _flush_cond in ntp_archiver");
            co_await _flush_cond.wait();
        } catch (const ss::broken_condition_variable&) {
            // We shutdown while waiting for a flush() to finish.
            // Return a failure result.
            vlog(
              _rtclog.debug,
              "Shutdown was issued during flush operation in ntp_archiver");
            co_return wait_result::failed;
        }
    }

    vlog(_rtclog.debug, "Flush was successfully completed in ntp_archiver.");
    co_return wait_result::complete;
}

bool ntp_archiver::stm_retention_needed() const {
    auto arch_so = manifest().get_archive_start_offset();
    // Return true if there is no archive
    return arch_so == model::offset{};
}

ss::future<> ntp_archiver::apply_retention() {
    if (!may_begin_uploads()) {
        co_return;
    }
    auto fence = emit_rw_fence();
    auto arch_so = manifest().get_archive_start_offset();
    auto stm_so = manifest().get_start_offset();
    if (arch_so != model::offset{} && arch_so != stm_so) {
        // We shouldn't do retention in the part of the log controlled by
        // the archival STM if archive region is not empty. It's unlikely for
        // STM retention and archive retention to work together. Most of the
        // time either STM retention will be happening without spillover or
        // archive retention will be happening alongside spillover from the STM.
        // This is just a safety check that prevents situation when the method
        // is called for log with not empty archive. In this case the retention
        // will remove too much data from the STM region of the log.
        vlog(
          _rtclog.warn,
          "Archive start offset {} is not equal to STM start offset {}, "
          "skipping STM retention",
          arch_so,
          stm_so);
        co_return;
    }

    if (manifest().archive_size_bytes() != 0) {
        vlog(
          _rtclog.error,
          "Size of the archive is not 0, but archival and STM start offsets "
          "are equal ({}). Skipping retention within STM region.",
          arch_so);
        co_return;
    }

    auto pinned_offset
      = _parent.raft()->log()->stm_hookset()->lowest_pinned_data_offset();
    auto retention_calculator = retention_calculator::factory(
      manifest(), _parent.get_ntp_config(), pinned_offset);
    if (!retention_calculator) {
        co_return;
    }

    auto next_start_offset = retention_calculator->next_start_offset();
    if (next_start_offset) {
        vlog(
          _rtclog.info,
          "{} Advancing start offset to {} satisfy retention policy",
          retention_calculator->strategy_name(),
          *next_start_offset);

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            // Currently, the 'unsafe_add' is always set to 'false'
            // because the fence is generated inside this method. It's still
            // good to have this condition in case if this will be changed.
            vlog(
              _rtclog.debug,
              "truncate, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.truncate(*next_start_offset);

        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.warn,
              "Failed to update archival metadata STM start offset according "
              "to retention policy: {}",
              error);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Failed to update start offset: {}", error));
        }
    } else {
        vlog(
          _rtclog.debug,
          "{} Retention policies are already met.",
          retention_calculator->strategy_name());
    }
}

ss::future<> ntp_archiver::garbage_collect() {
    if (!may_begin_uploads()) {
        co_return;
    }

    const auto to_remove
      = _parent.archival_meta_stm()->get_segments_to_cleanup();

    // Avoid replicating 'cleanup_metadata_cmd' if there's nothing to remove.
    if (to_remove.size() == 0) {
        co_return;
    }

    archival_stm_fence fence = emit_rw_fence();

    // If we are about to delete segments, we must ensure that the remote
    // manifest is fully up to date, so that it is definitely not referring
    // to any of the segments we will delete in its list of active segments.
    //
    // This is so that read replicas can be sure that if they get a 404
    // on a segment and go re-read the manifest, the latest manifest will
    // not refer to the non-existent segment (apart from in its 'replaced'
    // list)
    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      != cluster::archival_metadata_stm::state_dirty::clean) {
        // Intentionally not using maybe_upload_manifest, because  that would
        // skip the upload if manifest_upload_interval was not satisfied.
        auto result = co_await upload_manifest("pre-garbage-collect");
        if (result != cloud_storage::upload_result::success) {
            // If we could not write the  manifest, it is not safe to remove
            // segments.
            co_return;
        }
    }

    std::deque<cloud_storage_clients::object_key> objects_to_remove;
    for (const auto& meta : to_remove) {
        const auto path = manifest().generate_segment_path(
          meta, remote_path_provider());
        vlog(_rtclog.info, "Deleting segment from cloud storage: {}", path);

        objects_to_remove.emplace_back(path);
        objects_to_remove.emplace_back(
          cloud_storage::generate_remote_tx_path(path));
        objects_to_remove.emplace_back(
          cloud_storage::generate_index_path(path));
    }

    retry_chain_node fib(
      _conf->garbage_collect_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &_rtcnode);
    const auto delete_result = co_await _remote.delete_objects(
      get_bucket_name(), objects_to_remove, fib);

    const auto backlog_size_exceeded = to_remove.size()
                                       > _max_segments_pending_deletion();
    const auto all_deletes_succeeded = delete_result
                                       == cloud_storage::upload_result::success;
    if (!all_deletes_succeeded && backlog_size_exceeded) {
        vlog(
          _rtclog.warn,
          "The current number of segments pending deletion has exceeded the "
          "configurable limit ({} > {}) and deletion of some segments failed. "
          "Metadata for all remaining segments pending deletion will be "
          "removed and these segments will have to be removed manually.",
          to_remove.size(),
          _max_segments_pending_deletion());
    }

    if (all_deletes_succeeded || backlog_size_exceeded) {
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "cleanup_metadata, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.cleanup_metadata();
        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.info,
              "Failed to clean up metadata after garbage collection: {}",
              error);

            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Failed to clean up metadata after GC: {}", error));
        }
    } else {
        vlog(
          _rtclog.info,
          "Failed to delete all selected segments from cloud storage. Will "
          "retry on the next housekeeping run.");
    }

    _probe.value().segments_deleted(
      static_cast<int64_t>(all_deletes_succeeded ? to_remove.size() : 0));
    vlog(
      _rtclog.debug,
      "Deleted {} segments from the cloud",
      all_deletes_succeeded ? to_remove.size() : 0);
}

const cloud_storage_clients::bucket_name&
ntp_archiver::get_bucket_name() const {
    if (_bucket_override) {
        return *_bucket_override;
    } else {
        return _conf->bucket_name;
    }
}

std::vector<std::reference_wrapper<housekeeping_job>>
ntp_archiver::get_housekeeping_jobs() {
    std::vector<std::reference_wrapper<housekeeping_job>> res;
    if (_local_segment_merger) {
        res.emplace_back(std::ref(*_local_segment_merger));
    }

    if (_scrubber) {
        res.emplace_back(std::ref(*_scrubber));
    }

    return res;
}

ss::future<ntp_archiver::find_reupload_candidate_result>
ntp_archiver::find_reupload_candidate(
  manifest_scanner_t scanner, ss::abort_source& caller_as) {
    ss::gate::holder holder(_gate);

    ssx::composite_abort_source cas{caller_as, _as};

    archival_stm_fence rw_fence = emit_rw_fence();

    if (!may_begin_uploads()) {
        co_return find_reupload_candidate_result{};
    }
    auto run = scanner(_parent.raft_start_offset(), manifest());
    if (!run.has_value()) {
        vlog(_rtclog.debug, "Scan didn't resulted in upload candidate");
        co_return find_reupload_candidate_result{};
    } else {
        vlog(_rtclog.debug, "Scan result: {}", run);
    }
    auto units = co_await _mutex.get_units(cas.as());
    if (run->meta.base_offset >= _parent.raft_start_offset()) {
        auto log_generic = _parent.log();
        auto& log = *log_generic;
        segment_collector collector(
          segment_collector_mode::non_compacted_reupload,
          run->meta.base_offset,
          manifest(),
          log,
          // We want to upload exactly the same range as in the run we got based
          // on the manifest so do not limit collected range on the size.
          std::numeric_limits<size_t>::max(),
          run->meta.committed_offset);
        collector.collect_segments();
        auto candidate = co_await collector.make_upload_candidate_stream(
          _conf->segment_upload_timeout());

        co_return ss::visit(
          candidate,
          [](std::monostate) -> find_reupload_candidate_result {
              vassert(
                false,
                "unexpected default re-upload candidate creation result");
          },
          [this, &run, &rw_fence, units = std::move(units)](
            segment_collector_stream& collector_stream) mutable
            -> find_reupload_candidate_result {
              if (
                collector_stream.start_offset != run->meta.base_offset
                || collector_stream.end_offset != run->meta.committed_offset) {
                  vlog(
                    _rtclog.error,
                    "Failed to make reupload candidate to match the run, "
                    "candidate: {} run: {}",
                    collector_stream,
                    run->meta);
                  return {};
              }
              if (collector_stream.size != run->meta.size_bytes) {
                  vlog(
                    _rtclog.debug,
                    "Failed to make reupload candidate due to size mismatch, "
                    "skip this range: expected size: {}, actual size: {}",
                    human::bytes(run->meta.size_bytes),
                    human::bytes(collector_stream.size));
                  return {.skip_to = collector_stream.end_offset};
              }
              return {
                .units = std::move(units),
                .upload_stream = std::move(collector_stream),
                .read_write_fence = rw_fence};
          },
          [this](
            skip_offset_range& skip_offsets) -> find_reupload_candidate_result {
              const auto log_level = log_level_for_error(skip_offsets.reason);
              vlogl(
                _rtclog,
                log_level,
                "Failed to make reupload candidate: {}",
                skip_offsets.reason);
              return {};
          },
          [this](
            candidate_creation_error& error) -> find_reupload_candidate_result {
              const auto log_level = log_level_for_error(error);
              vlogl(
                _rtclog,
                log_level,
                "Failed to make reupload candidate: {}",
                error);
              return {};
          });
    }
    // OTHERWISE WE'RE REUPLOADING REMOTE SEGMENTS
    // This is not currently supported, and is tricky to wire up to the
    // stream-based interface, so just elide all this code for now and return an
    // empty result
    co_return find_reupload_candidate_result{};
}

cloud_storage::segment_name ntp_archiver::segment_name_for_stream(
  const segment_collector_stream& strm,
  std::optional<model::term_id> archiver_term) {
    auto term = archiver_term.value_or(_start_term);
    auto meta = convert_segment_meta(strm, _parent, _rev, term);
    return cloud_storage::partition_manifest::generate_remote_segment_name(
      meta);
}

ss::future<bool> ntp_archiver::upload(
  find_reupload_candidate_result find_res,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    ss::gate::holder holder(_gate);
    if (!find_res.upload_stream.has_value() || !find_res.units.has_value()) {
        // The method shouldn't be called if this is the case
        co_return false;
    }
    auto units = std::move(find_res.units);
    if (find_res.upload_stream.value().size > 0) {
        co_return co_await do_upload_local(
          find_res.read_write_fence,
          std::move(find_res.upload_stream).value(),
          source_rtc);
    }
    // Currently, the uploading of remote segments is disabled and
    // the only reason why the list of locks is empty is truncation.
    // The log could be truncated right after we scanned the manifest to
    // find upload candidate. In this case we will get an empty candidate
    // which is not a failure so we shuld return 'true'.
    co_return true;
}

ss::future<bool> ntp_archiver::do_upload_local(
  archival_stm_fence fence,
  segment_collector_stream strm,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    if (!may_begin_uploads()) {
        co_return false;
    }
    if (!config::shard_local_cfg().cloud_storage_enable_segment_uploads()) {
        co_return false;
    }

    auto sname = segment_name_for_stream(strm);

    if (strm.is_compacted) {
        vlog(
          _rtclog.warn,
          "Upload of {} requested but sources are compacted",
          sname);
        co_return false;
    }

    if (strm.size == 0) {
        vlog(
          _rtclog.warn,
          "Upload of the {} requested but sources are empty",
          sname);
        co_return false;
    }

    auto meta = convert_segment_meta(strm, _parent, _rev, _start_term);
    auto [tx_ranges, tx_size] = co_await get_aborted_transactions(strm, sname);
    meta.metadata_size_hint = tx_size;
    vlog(
      _rtclog.debug,
      "Starting segment upload in the background, name: {}, meta: {}",
      sname,
      meta);

    auto upl_res = co_await upload_segment(
      std::move(strm), meta, std::move(tx_ranges));

    if (upl_res.result() != cloud_storage::upload_result::success) {
        vlog(
          _rtclog.warn,
          "Failed to upload segment: {}, error: {}",
          sname,
          upl_res.result());
        co_return false;
    }

    const bool checks_disabled
      = config::shard_local_cfg()
          .cloud_storage_disable_upload_consistency_checks.value();

    if (!checks_disabled && upl_res.has_record_stats()) {
        auto stats = upl_res.record_stats();
        // Validate segment content. The 'stats' is computed when
        // the actual segment is scanned and represents the 'ground truth' about
        // its content. The 'meta' is the expected segment metadata. We
        // shouldn't replicate it if it doesn't match the 'stats'.
        if (!segment_meta_matches_stats(
              meta,
              stats,
              _rtclog,
              _parent.get_ntp_config().is_remote_allow_gaps_enabled())) {
            co_return false;
        }
    }
    if (!checks_disabled) {
        // Validate metadata using the STM state
        if (!manifest().safe_segment_meta_to_add(meta)) {
            co_return false;
        }
    }

    auto highest_producer_id
      = _feature_table.local().is_active(
          features::feature::cloud_metadata_cluster_recovery)
          ? _parent.highest_producer_id()
          : model::producer_id{};
    auto deadline = ss::lowres_clock::now() + _conf->manifest_upload_timeout();

    auto is_validated = checks_disabled ? cluster::segment_validated::no
                                        : cluster::segment_validated::yes;
    cluster::emit_read_write_fence rw_fence = std::nullopt;
    if (fence.emit_rw_fence_cmd) {
        vlog(
          archival_log.debug,
          "(2) fence value is: {}, manifest last applied "
          "offset: {}, manifest in-sync offset: {}",
          fence.read_write_fence,
          _parent.archival_meta_stm()->manifest().get_applied_offset(),
          _parent.archival_meta_stm()->get_insync_offset());
        rw_fence = fence.read_write_fence;
    }

    auto error = co_await _parent.archival_meta_stm()->add_segments(
      {meta},
      std::nullopt,
      highest_producer_id,
      deadline,
      _as,
      is_validated,
      rw_fence);

    if (error != cluster::errc::success && error != cluster::errc::not_leader) {
        vlog(
          _rtclog.warn,
          "archival metadata STM update failed: {}",
          error.message());
        co_return false;
    }

    if (
      co_await upload_manifest(segment_merger_ctx_label, source_rtc)
      != cloud_storage::upload_result::success) {
        vlog(
          _rtclog.info,
          "archival metadata replicated but manifest is not re-uploaded");
    } else {
        // Write to archival_metadata_stm to mark our updated clean offset
        // as a result of uploading the manifest successfully.
        co_await flush_manifest_clean_offset();
    }
    co_return true;
}

ss::future<bool> ntp_archiver::do_upload_remote(
  upload_candidate_with_locks candidate,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    std::ignore = candidate;
    std::ignore = source_rtc;
    throw std::runtime_error("Not implemented");
}

size_t ntp_archiver::get_local_segment_size() const {
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(
      *_parent.raft()->log());

    return disk_log.max_segment_size();
}

ss::future<bool>
ntp_archiver::prepare_transfer_leadership(ss::lowres_clock::duration timeout) {
    _paused = true;

    ss::gate::holder holder(_gate);
    try {
        auto units = co_await _uploads_active.get_units(timeout);
        vlog(
          _rtclog.trace,
          "prepare_transfer_leadership: got units (current {})",
          _uploads_active.has_units());
    } catch (const ss::semaphore_timed_out&) {
        // In this situation, it is possible that the old leader (this node)
        // will leave an orphan object behind in object storage, because
        // the next manifest written by the new leader will not refer to
        // this object.
        //
        // This is not a correctness issue, but consumes some disk space,
        // and these objects may also be left behind when the topic is later
        // deleted.
        co_return false;
    }

    // Attempt to flush our clean offset, to avoid the new leader redundantly
    // uploading a copy of the manifest based on a stale clean offset in
    // the stm.  This is an optimization: if it fails then the leader transfer
    // will still proceed smoothly, there just may be an extra manifest upload
    // on the new leader.
    co_await flush_manifest_clean_offset();

    co_return true;
}

const storage::ntp_config& ntp_archiver::ntp_config() const {
    return _parent.log()->config();
}

const cloud_storage::remote_path_provider&
ntp_archiver::remote_path_provider() const {
    return _parent.archival_meta_stm()->path_provider();
}

void ntp_archiver::complete_transfer_leadership() {
    vlog(
      _rtclog.trace,
      "complete_transfer_leadership: current units (current {})",
      _uploads_active.has_units());
    _paused = false;
    _leader_cond.signal();
}

bool ntp_archiver::lost_leadership() const {
    return !_parent.is_leader() || _parent.term() != _start_term;
}

bool ntp_archiver::local_storage_pressure() const {
    auto eviction_offset = _parent.eviction_requested_offset();

    return eviction_offset.has_value()
           && _parent.archival_meta_stm()->get_last_clean_at()
                <= eviction_offset.value();
}

bool ntp_archiver::flush_in_progress() const {
    return _flush_uploads_offset.has_value();
}

bool ntp_archiver::uploaded_and_clean_past_offset(model::offset o) const {
    vlog(
      _rtclog.trace,
      "In uploaded_and_clean_past_offset(), manifest last "
      "offset: {}, last clean offset: {}, query offset: {}.",
      manifest().get_last_offset(),
      _parent.archival_meta_stm()->get_last_clean_at(),
      o);
    return std::min(
             manifest().get_last_offset(),
             _parent.archival_meta_stm()->get_last_clean_at())
           >= o;
}

bool ntp_archiver::uploaded_data_past_flush_offset() const {
    return flush_in_progress()
           && manifest().get_last_offset() >= _flush_uploads_offset.value();
}

void ntp_archiver::initialize_probe() {
    _probe.emplace(
      _conf->ntp_metrics_disabled, _ntp, _parent.archival_meta_stm());
}

} // namespace archival
