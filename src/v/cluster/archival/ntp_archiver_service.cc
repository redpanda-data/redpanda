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

bool emit_read_write_fence(
  const ss::sharded<features::feature_table>& feature_table) {
    return !config::shard_local_cfg()
              .cloud_storage_disable_archival_stm_rw_fence.value()
           && feature_table.local().is_active(
             features::feature::cloud_storage_metadata_rw_fence);
}

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
    if (unlikely(
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

        if (auto ec = co_await maybe_repair_manifest(
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
    if (unlikely(
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

} // namespace archival
