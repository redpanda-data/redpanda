/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"

#include "archival/adjacent_segment_merger.h"
#include "archival/archival_policy.h"
#include "archival/logger.h"
#include "archival/retention_calculator.h"
#include "archival/segment_reupload.h"
#include "archival/types.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/record.h"
#include "raft/types.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/ntp_config.h"
#include "storage/parser.h"
#include "utils/human.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <exception>
#include <numeric>
#include <stdexcept>

namespace archival {

static std::unique_ptr<adjacent_segment_merger>
maybe_make_adjacent_segment_merger(
  ntp_archiver& self, retry_chain_logger& log, const storage::ntp_config& cfg) {
    std::unique_ptr<adjacent_segment_merger> result = nullptr;
    if (
      cfg.is_archival_enabled() && !cfg.is_compacted()
      && !cfg.is_read_replica_mode_enabled()) {
        result = std::make_unique<adjacent_segment_merger>(self, log, true);
        result->set_enabled(config::shard_local_cfg()
                              .cloud_storage_enable_segment_merging.value());
    }
    return result;
}

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp,
  ss::lw_shared_ptr<const configuration> conf,
  cloud_storage::remote& remote,
  cluster::partition& parent)
  : _ntp(ntp.ntp())
  , _rev(ntp.get_initial_revision())
  , _remote(remote)
  , _parent(parent)
  , _policy(_ntp, conf->time_limit, conf->upload_io_priority)
  , _gate()
  , _rtcnode(_as)
  , _rtclog(archival_log, _rtcnode, _ntp.path())
  , _conf(conf)
  , _sync_manifest_timeout(
      config::shard_local_cfg()
        .cloud_storage_readreplica_manifest_sync_timeout_ms.bind())
  , _max_segments_pending_deletion(
      config::shard_local_cfg()
        .cloud_storage_max_segments_pending_deletion_per_partition.bind())
  , _housekeeping_interval(
      config::shard_local_cfg().cloud_storage_housekeeping_interval_ms.bind())
  , _housekeeping_jitter(_housekeeping_interval(), 5ms)
  , _next_housekeeping(_housekeeping_jitter())
  , _segment_tags(cloud_storage::remote::make_segment_tags(_ntp, _rev))
  , _manifest_tags(
      cloud_storage::remote::make_partition_manifest_tags(_ntp, _rev))
  , _tx_tags(cloud_storage::remote::make_tx_manifest_tags(_ntp, _rev))
  , _segment_index_tags(
      cloud_storage::remote::make_segment_index_tags(_ntp, _rev))
  , _local_segment_merger(
      maybe_make_adjacent_segment_merger(*this, _rtclog, parent.log().config()))
  , _segment_merging_enabled(
      config::shard_local_cfg().cloud_storage_enable_segment_merging.bind())
  , _manifest_upload_interval(
      config::shard_local_cfg()
        .cloud_storage_manifest_max_upload_interval_sec.bind())
  , _feature_table(parent.feature_table()) {
    _start_term = _parent.term();
    // Override bucket for read-replica
    if (_parent.is_read_replica_mode_enabled()) {
        _bucket_override = _parent.get_read_replica_bucket();
    } else if (
      parent.log().config().is_archival_enabled()
      && !parent.log().config().is_compacted()) {
        _segment_merging_enabled.watch([this] {
            _local_segment_merger->set_enabled(_segment_merging_enabled());
        });
    }

    vlog(
      archival_log.debug,
      "created ntp_archiver {} in term {}",
      _ntp,
      _start_term);
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
        ssx::spawn_with_gate(
          _gate, [this] { return sync_manifest_until_abort(); });
    } else {
        ssx::spawn_with_gate(_gate, [this] { return upload_until_abort(); });
    }

    return ss::now();
}

void ntp_archiver::notify_leadership(std::optional<model::node_id> leader_id) {
    if (leader_id && *leader_id == _parent.raft()->self().id()) {
        _leader_cond.signal();
    }
}

ss::future<> ntp_archiver::upload_until_abort() {
    if (!_probe) {
        _probe.emplace(_conf->ntp_metrics_disabled, _ntp);
    }

    while (!_as.abort_requested()) {
        if (!_parent.is_leader() || _paused) {
            bool shutdown = false;
            try {
                vlog(
                  _rtclog.debug, "upload loop waiting for leadership/unpause");
                co_await _leader_cond.wait();
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
        if (!may_begin_uploads()) {
            continue;
        }
        vlog(_rtclog.debug, "upload loop starting in term {}", _start_term);
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        bool is_synced = co_await _parent.archival_meta_stm()->sync(
          sync_timeout);
        if (!is_synced) {
            co_return;
        }
        vlog(_rtclog.debug, "upload loop synced in term {}", _start_term);

        co_await ss::with_scheduling_group(
          _conf->upload_scheduling_group,
          [this] { return upload_until_term_change(); })
          .handle_exception_type([](const ss::abort_requested_exception&) {})
          .handle_exception_type([](const ss::sleep_aborted&) {})
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
    }
}

ss::future<> ntp_archiver::sync_manifest_until_abort() {
    if (!_probe) {
        _probe.emplace(_conf->ntp_metrics_disabled, _ntp);
    }

    while (!_as.abort_requested()) {
        if (!_parent.is_leader() || _paused) {
            bool shutdown = false;
            try {
                vlog(
                  _rtclog.debug,
                  "sync manifest loop waiting for leadership/unpause");
                co_await _leader_cond.wait();
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
              .handle_exception_type([](const ss::sleep_aborted&) {})
              .handle_exception_type([](const ss::gate_closed_exception&) {});
        } catch (const ss::semaphore_timed_out& e) {
            vlog(
              _rtclog.warn,
              "Semaphore timed out in the upload loop: {}. This may be "
              "due to the system being overloaded. The loop will "
              "restart.",
              e);
        } catch (...) {
            vlog(
              _rtclog.error, "upload loop error: {}", std::current_exception());
        }
    }
}

/// Helper for generating topic manifest to upload
static cloud_storage::manifest_topic_configuration convert_topic_configuration(
  const cluster::topic_configuration& cfg,
  cluster::replication_factor replication_factor) {
    cloud_storage::manifest_topic_configuration result {
      .tp_ns = cfg.tp_ns,
      .partition_count = cfg.partition_count,
      .replication_factor = replication_factor,
      .properties = {
        .compression = cfg.properties.compression,
        .cleanup_policy_bitflags = cfg.properties.cleanup_policy_bitflags,
        .compaction_strategy = cfg.properties.compaction_strategy,
        .timestamp_type = cfg.properties.timestamp_type,
        .segment_size = cfg.properties.segment_size,
        .retention_bytes = cfg.properties.retention_bytes,
        .retention_duration = cfg.properties.retention_duration,
      },
    };
    return result;
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

    auto replication_factor = cluster::replication_factor{
      _parent.raft()->config().current_config().voters.size()};

    try {
        retry_chain_node fib(
          _conf->manifest_upload_timeout,
          _conf->cloud_storage_initial_backoff,
          &_rtcnode);
        retry_chain_logger ctxlog(archival_log, fib);
        vlog(ctxlog.info, "Uploading topic manifest {}", _parent.ntp());
        cloud_storage::topic_manifest tm(
          convert_topic_configuration(topic_cfg, replication_factor), _rev);
        auto key = tm.get_manifest_path();
        vlog(ctxlog.debug, "Topic manifest object key is '{}'", key);
        auto res = co_await _remote.upload_manifest(
          _conf->bucket_name, tm, fib);
        if (res != cloud_storage::upload_result::success) {
            vlog(ctxlog.warn, "Topic manifest upload failed: {}", key);
        } else {
            _topic_manifest_dirty = false;
        }
    } catch (const ss::gate_closed_exception& err) {
    } catch (const ss::abort_requested_exception& err) {
    } catch (...) {
        vlog(
          _rtclog.warn,
          "Error writing topic manifest for {}: {}",
          _parent.ntp(),
          std::current_exception());
    }
}

ss::future<> ntp_archiver::upload_until_term_change() {
    ss::lowres_clock::duration backoff = _conf->upload_loop_initial_backoff;

    // Before starting, upload the manifest if needed.  This makes our
    // behavior more deterministic on first start (uploading the empty
    // manifest) and after unclean leadership changes (flush dirty manifest
    // as soon as we can, rather than potentially waiting for segment
    // uploads).
    {
        auto units = co_await ss::get_units(_uploads_active, 1);
        co_await maybe_upload_manifest(upload_loop_prologue_ctx_label);
        co_await flush_manifest_clean_offset();
    }

    while (may_begin_uploads()) {
        // Hold sempahore units to enable other code to know that we are in
        // the process of doing uploads + wait for us to drop out if they
        // e.g. set _paused.
        vassert(!_paused, "may_begin_uploads must ensure !_paused");
        auto units = co_await ss::get_units(_uploads_active, 1);
        vlog(
          _rtclog.trace,
          "upload_until_term_change: got units (current {}), paused={}",
          _uploads_active.current(),
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
        bool is_synced = co_await _parent.archival_meta_stm()->sync(
          sync_timeout);
        if (!is_synced) {
            // This can happen on leadership changes, or on timeouts waiting
            // for stm to catch up: in either case, we should re-check our
            // loop condition: we will drop out if lost leadership, otherwise
            // we will end up back here to try the sync again.
            continue;
        }

        auto [non_compacted_upload_result, compacted_upload_result]
          = co_await upload_next_candidates();
        if (non_compacted_upload_result.num_failed != 0) {
            // The logic in class `remote` already does retries: if we get here,
            // it means the upload failed after several retries, justifying
            // a warning to the operator: something non-transient may be wrong,
            // although we do also see this in practice on AWS S3 occasionally
            // during normal operation.
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
              "Successfuly uploaded {} segments",
              non_compacted_upload_result.num_succeeded);
        }

        if (non_compacted_upload_result.num_cancelled != 0) {
            vlog(
              _rtclog.debug,
              "Cancelled upload of {} segments",
              non_compacted_upload_result.num_cancelled);
        }

        if (ss::lowres_clock::now() >= _next_housekeeping) {
            co_await housekeeping();
            _next_housekeeping = _housekeeping_jitter();
        }

        if (!may_begin_uploads()) {
            break;
        }

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

        update_probe();

        // Drop _uploads_active lock: we are not considered active while
        // sleeping for backoff at the end of the loop.
        units.return_all();
        vlog(
          _rtclog.trace,
          "upload_until_term_change: released units (current {})",
          _uploads_active.current());

        if (non_compacted_upload_result.num_succeeded == 0) {
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
            co_await ss::sleep_abortable(
              backoff + _backoff_jitter.next_jitter_duration(), _as);
            backoff = std::min(backoff * 2, _conf->upload_loop_max_backoff);
        } else {
            backoff = _conf->upload_loop_initial_backoff;
        }
    }
}

ss::future<> ntp_archiver::sync_manifest_until_term_change() {
    while (can_update_archival_metadata()) {
        cloud_storage::download_result result = co_await sync_manifest();

        if (result != cloud_storage::download_result::success) {
            // The logic in class `remote` already does retries: if we get here,
            // it means the download failed after several retries, indicating
            // something non-transient may be wrong. Hence error severity.
            vlog(
              _rtclog.error,
              "Failed to download manifest {}",
              manifest().get_manifest_path());
        } else {
            vlog(
              _rtclog.debug,
              "Successfuly downloaded manifest {}",
              manifest().get_manifest_path());
        }
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
        vlog(
          _rtclog.debug,
          "Updating the archival_meta_stm in read-replica mode, in-sync "
          "offset: {}, last uploaded offset: {}, last compacted offset: {}",
          m.get_insync_offset(),
          m.get_last_offset(),
          m.get_last_uploaded_compacted_offset());

        if (m.get_last_offset() < manifest().get_last_offset()) {
            // This indicates time travel, possible because the source cluster
            // had a stale leader node upload a manifest on top of a more
            // recent manifest uploaded by the current leader.  This is legal
            // and the reader (us) should ignore the apparent time travel,
            // in expectation that the current leader eventually wins and
            // uploads a more recent manifest.
            vlog(
              _rtclog.error,
              "Ignoring remote manifest.json contents: last_offset {} behind "
              "last"
              "seen last_offset {} (remote insync_offset {})",
              m.get_last_offset(),
              manifest().get_last_offset(),
              m.get_insync_offset());
            co_return res;
        }

        std::vector<cloud_storage::segment_meta> mdiff;
        // Several things has to be done:
        // - Add all segments between old last_offset and new last_offset
        // - Compare all segments below last compacted offset with their
        //   counterparts in the old manifest and re-add them if they are
        //   diferent.
        // - Apply new start_offset if it's different
        auto offset = model::next_offset(manifest().get_last_offset());
        for (auto it = m.segment_containing(offset); it != m.end(); it++) {
            mdiff.push_back(it->second);
        }

        bool needs_cleanup = false;
        auto old_start_offset = manifest().get_start_offset();
        auto new_start_offset = m.get_start_offset();
        for (const auto& s : m) {
            if (
              s.second.committed_offset
                <= m.get_last_uploaded_compacted_offset()
              && s.second.base_offset >= new_start_offset) {
                // Re-uploaded segments has to be aligned with one of
                // the existing segments in the manifest. This is guaranteed
                // by the archiver. Because of that we can simply lookup
                // the base offset of the segment in the manifest and
                // compare them.
                auto iter = manifest().get(s.first);
                if (iter && *iter != s.second) {
                    mdiff.push_back(s.second);
                    needs_cleanup = true;
                }
            } else {
                break;
            }
        }

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        // The commands to update the manifest need to be batched together,
        // otherwise the read-replica will be able to see partial update. Also,
        // the batching is more efficient.
        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        builder.add_segments(std::move(mdiff));
        if (
          new_start_offset.has_value() && old_start_offset.has_value()
          && old_start_offset.value() != new_start_offset.value()) {
            builder.truncate(new_start_offset.value());
            needs_cleanup = true;
        }
        if (needs_cleanup) {
            // We only need to replicate this command if the
            // manifest will be truncated or compacted segments
            // will be added by previous commands.
            builder.cleanup_metadata();
        }
        auto errc = co_await builder.replicate();
        if (errc) {
            if (errc == raft::errc::shutting_down) {
                // During shutdown, act like we hit an abort source rather
                // than trying to log+handle this like a write error.
                throw ss::abort_requested_exception();
            }

            vlog(
              _rtclog.error,
              "Can't replicate archival_metadata_stm configuration batch: "
              "{}",
              errc.message());
            co_return cloud_storage::download_result::failed;
        }
    }

    _last_sync_time = ss::lowres_clock::now();
    co_return cloud_storage::download_result::success;
}

void ntp_archiver::update_probe() {
    const auto& man = manifest();

    _probe->segments_in_manifest(man.size());

    const auto first_addressable = man.first_addressable_segment();
    const auto truncated_seg_count = first_addressable == man.end()
                                       ? 0
                                       : std::distance(
                                         man.begin(), first_addressable);

    _probe->segments_to_delete(
      truncated_seg_count + man.replaced_segments_count());
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
    _as.request_abort();
    _uploads_active.broken();
    _leader_cond.broken();
    co_await _gate.close();
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
    gate_guard guard{_gate};
    retry_chain_node fib(
      _conf->manifest_upload_timeout,
      _conf->cloud_storage_initial_backoff,
      &_rtcnode);
    cloud_storage::partition_manifest tmp(_ntp, _rev);
    auto path = tmp.get_manifest_path();
    auto key = cloud_storage::remote_manifest_path(
      std::filesystem::path(std::move(path)));
    vlog(_rtclog.debug, "Downloading manifest");
    auto result = co_await _remote.download_manifest(
      get_bucket_name(), key, tmp, fib);

    // It's OK if the manifest is not found for a newly created topic. The
    // condition in if statement is not guaranteed to cover all cases for new
    // topics, so false positives may happen for this warn.
    if (
      result == cloud_storage::download_result::notfound
      && _parent.high_watermark() != model::offset(0)
      && _parent.term() != model::term_id(1)) {
        vlog(
          _rtclog.warn,
          "Manifest for {} not found in S3, partition high_watermark: {}, "
          "partition term: {}",
          _ntp,
          _parent.high_watermark(),
          _parent.term());
    }

    co_return std::make_pair(tmp, result);
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
    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      == cluster::archival_metadata_stm::state_dirty::clean) {
        vlog(
          _rtclog.debug,
          "[{}] Manifest is clean{}, skipping upload",
          upload_ctx,
          _projected_manifest_clean_at.has_value() ? " (projected)" : "");
        co_return false;
    }

    // Do not upload if interval has not elapsed.  The upload loop will call
    // us again periodically and we will eventually upload when we pass the
    // interval.  Make an exception if we are under local storage pressure,
    // and skip the interval wait in this case.
    if (
      !local_storage_pressure() && _manifest_upload_interval().has_value()
      && ss::lowres_clock::now() - _last_manifest_upload_time
           < _manifest_upload_interval().value()) {
        co_return false;
    }

    auto result = co_await upload_manifest(upload_ctx);
    co_return result == cloud_storage::upload_result::success;
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

    gate_guard guard{_gate};
    auto rtc = source_rtc.value_or(std::ref(_rtcnode));
    retry_chain_node fib(
      _conf->manifest_upload_timeout,
      _conf->cloud_storage_initial_backoff,
      &rtc.get());
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    auto units = co_await _parent.archival_meta_stm()->acquire_manifest_lock();

    auto upload_insync_offset = manifest().get_insync_offset();

    vlog(
      _rtclog.debug,
      "[{}] Uploading partition manifest, insync_offset={}, path={}",
      upload_ctx,
      upload_insync_offset,
      manifest().get_manifest_path());

    auto result = co_await _remote.upload_manifest(
      get_bucket_name(), manifest(), fib, _manifest_tags);

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

remote_segment_path
ntp_archiver::segment_path_for_candidate(const upload_candidate& candidate) {
    vassert(
      candidate.remote_sources.empty(),
      "This method can only work with local segments");
    auto front = candidate.sources.front();
    cloud_storage::partition_manifest::value val{
      .is_compacted = front->is_compacted_segment()
                      && front->finished_self_compaction(),
      .size_bytes = candidate.content_length,
      .base_offset = candidate.starting_offset,
      .committed_offset = candidate.final_offset,
      .ntp_revision = _rev,
      .archiver_term = _start_term,
      .segment_term = candidate.term,
      .sname_format = cloud_storage::segment_name_format::v3,
    };

    return manifest().generate_segment_path(val);
}

static std::pair<ss::input_stream<char>, ss::input_stream<char>>
split_segment_stream(
  upload_candidate candidate, ss::io_priority_class priority) {
    return input_stream_fanout<2>(
      storage::concat_segment_reader_view{
        candidate.sources,
        candidate.file_offset,
        candidate.final_file_offset,
        priority}
        .take_stream(),
      config::shard_local_cfg().storage_read_readahead_count());
}

ss::future<cloud_storage::upload_result> ntp_archiver::do_upload_segment(
  upload_candidate candidate,
  stream_reference& stream_state,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    vassert(
      stream_state.stream_ref.has_value(),
      "Stream passed to upload segment does not have a value");
    gate_guard guard{_gate};

    auto rtc = source_rtc.value_or(std::ref(_rtcnode));
    retry_chain_node fib(
      _conf->segment_upload_timeout,
      _conf->cloud_storage_initial_backoff,
      &rtc.get());
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    auto path = segment_path_for_candidate(candidate);
    vlog(ctxlog.debug, "Uploading segment {} to {}", candidate, path);

    auto lazy_abort_source = cloud_storage::lazy_abort_source{
      [this]() { return upload_should_abort(); },
    };

    // This struct wraps a stream object and exposes the stream_provider
    // interface for compatibility with the remote object API.
    struct stream_wrapper final : public storage::stream_provider {
        stream_reference::stream_ref_t stream;

        stream_wrapper(const stream_wrapper&) = delete;
        stream_wrapper(stream_wrapper&&) = default;
        stream_wrapper& operator=(const stream_wrapper&) = delete;
        stream_wrapper& operator=(stream_wrapper&&) = default;

        ~stream_wrapper() override = default;

        explicit stream_wrapper(ss::input_stream<char>& s)
          : stream(s) {}

        ss::input_stream<char> take_stream() override {
            vassert(stream.has_value(), "no stream to take");
            ss::input_stream<char> s = std::move(stream.value().get());
            stream = std::nullopt;
            return s;
        }

        ss::future<> close() override {
            if (stream.has_value()) {
                co_await stream.value().get().close();
            }
            co_return;
        }
    };

    auto reset_func = [this, candidate, &stream_state] {
        using provider_t = std::unique_ptr<storage::stream_provider>;
        // On first attempt to upload, the stream-ref passed in is used.
        if (stream_state.stream_ref.has_value()) {
            auto f = ss::make_ready_future<provider_t>(
              std::make_unique<stream_wrapper>(stream_state.stream_ref->get()));
            stream_state.stream_ref = std::nullopt;
            return f;
        } else {
            // On subsequent uploads, the segment is read again from disk
            return ss::make_ready_future<provider_t>(
              std::make_unique<storage::concat_segment_reader_view>(
                candidate.sources,
                candidate.file_offset,
                candidate.final_file_offset,
                _conf->upload_io_priority));
        }
    };

    co_return co_await _remote.upload_segment(
      get_bucket_name(),
      path,
      candidate.content_length,
      std::move(reset_func),
      fib,
      lazy_abort_source,
      _segment_tags);
}

ss::future<> ntp_archiver::stream_reference::maybe_close_ref() {
    if (stream_ref.has_value()) {
        co_await stream_ref->get().close();
    }
}

static ss::sstring make_index_path(const remote_segment_path& segment_path) {
    return fmt::format("{}.index", segment_path().native());
}

// from offset to offset (by record batch boundary)
ss::future<cloud_storage::upload_result> ntp_archiver::upload_segment(
  upload_candidate candidate,
  std::vector<ss::rwlock::holder> segment_read_locks,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    vassert(
      candidate.remote_sources.empty(),
      "This method can only work with local segments");

    auto [stream_upload, stream_index] = split_segment_stream(
      candidate, _conf->upload_io_priority);

    auto path = segment_path_for_candidate(candidate);
    stream_reference stream_state{.stream_ref = stream_upload};

    auto upload_fut
      = do_upload_segment(candidate, stream_state, source_rtc)
          .handle_exception_type(
            [&stream_state](const ss::gate_closed_exception&) mutable {
                return stream_state.maybe_close_ref().then(
                  [] { return cloud_storage::upload_result::cancelled; });
            })
          .handle_exception_type(
            [&stream_state](const ss::abort_requested_exception&) mutable {
                return stream_state.maybe_close_ref().then(
                  [] { return cloud_storage::upload_result::cancelled; });
            })
          .handle_exception_type(
            [this, path, &stream_state](const std::exception& e) mutable {
                vlog(_rtclog.error, "failed to upload segment {}: {}", path, e);
                return stream_state.maybe_close_ref().then(
                  [] { return cloud_storage::upload_result::failed; });
            });

    auto index_path = make_index_path(path);
    auto make_idx_fut = make_segment_index(
      candidate.starting_offset, _rtclog, index_path, std::move(stream_index));

    auto [upload_res, idx_res] = co_await ss::when_all_succeed(
      std::move(upload_fut), std::move(make_idx_fut));

    if (
      upload_res == cloud_storage::upload_result::success
      && idx_res.has_value()) {
        auto rtc = source_rtc.value_or(std::ref(_rtcnode));
        retry_chain_node fib(
          _conf->segment_upload_timeout,
          _conf->cloud_storage_initial_backoff,
          &rtc.get());

        co_await _remote.upload_object(
          _conf->bucket_name,
          cloud_storage_clients::object_key{index_path},
          idx_res->to_iobuf(),
          fib,
          _segment_index_tags,
          "segment-index");
    } else {
        if (upload_res != cloud_storage::upload_result::success) {
            vlog(
              _rtclog.warn,
              "skipping segment index upload to {} because segment upload was "
              "unsuccessful: {}",
              index_path,
              path);
        }

        if (!idx_res.has_value()) {
            vlog(
              _rtclog.warn,
              "skipping segment index upload to {} index could not be "
              "generated",
              index_path);
        }
    }

    co_return upload_res;
    // Note on segment locks:
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

std::optional<ss::sstring> ntp_archiver::upload_should_abort() {
    auto original_term = _parent.term();
    auto lost_leadership = !_parent.is_leader()
                           || _parent.term() != original_term;
    if (unlikely(lost_leadership)) {
        return fmt::format(
          "lost leadership or term changed during upload, "
          "current leadership status: {}, "
          "current term: {}, "
          "original term: {}",
          _parent.is_leader(),
          _parent.term(),
          original_term);
    } else {
        return std::nullopt;
    }
}

ss::future<fragmented_vector<model::tx_range>>
ntp_archiver::get_aborted_transactions(upload_candidate candidate) {
    vassert(
      candidate.remote_sources.empty(),
      "This method can only work with local segments");
    gate_guard guard{_gate};
    co_return co_await _parent.aborted_transactions(
      candidate.starting_offset, candidate.final_offset);
}

ss::future<cloud_storage::upload_result> ntp_archiver::upload_tx(
  upload_candidate candidate,
  fragmented_vector<model::tx_range> tx_range,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    gate_guard guard{_gate};
    auto rtc = source_rtc.value_or(std::ref(_rtcnode));
    retry_chain_node fib(
      _conf->segment_upload_timeout,
      _conf->cloud_storage_initial_backoff,
      &rtc.get());
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    vlog(
      ctxlog.debug, "Uploading segment's tx range {}", candidate.exposed_name);

    if (tx_range.empty()) {
        // The actual upload only happens if tx_range is not empty.
        // The remote_segment should act as if the tx_range is empty if the
        // request returned NoSuchKey error.
        co_return cloud_storage::upload_result::success;
    }

    auto path = segment_path_for_candidate(candidate);

    cloud_storage::tx_range_manifest manifest(path, std::move(tx_range));

    co_return co_await _remote.upload_manifest(
      get_bucket_name(), manifest, fib, _tx_tags);
}

ss::future<std::optional<cloud_storage::offset_index>>
ntp_archiver::make_segment_index(
  model::offset base_rp_offset,
  retry_chain_logger& ctxlog,
  std::string_view index_path,
  ss::input_stream<char> stream) {
    auto base_kafka_offset = model::offset_cast(
      _parent.get_offset_translator_state()->from_log_offset(base_rp_offset));

    cloud_storage::offset_index ix{
      base_rp_offset,
      base_kafka_offset,
      0,
      cloud_storage::remote_segment_sampling_step_bytes};

    vlog(ctxlog.debug, "creating remote segment index: {}", index_path);
    auto builder = cloud_storage::make_remote_segment_index_builder(
      std::move(stream),
      ix,
      base_rp_offset - base_kafka_offset,
      cloud_storage::remote_segment_sampling_step_bytes);

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

    co_return ix;
}

// The function turns an array of futures that return an error code into a
// single future that returns error result of the last failed future or success
// otherwise.
static ss::future<cloud_storage::upload_result> aggregate_upload_results(
  std::vector<ss::future<cloud_storage::upload_result>> upl_vec) {
    return ss::when_all(upl_vec.begin(), upl_vec.end()).then([](auto vec) {
        auto res = cloud_storage::upload_result::success;
        for (auto& v : vec) {
            try {
                auto r = v.get();
                if (r != cloud_storage::upload_result::success) {
                    res = r;
                }
            } catch (const ss::gate_closed_exception&) {
                res = cloud_storage::upload_result::cancelled;
            } catch (...) {
                res = cloud_storage::upload_result::failed;
            }
        }
        return res;
    });
}

ss::future<ntp_archiver::scheduled_upload>
ntp_archiver::schedule_single_upload(const upload_context& upload_ctx) {
    auto start_upload_offset = upload_ctx.start_offset;
    auto last_stable_offset = upload_ctx.last_offset;

    auto log = _parent.log();

    upload_candidate_with_locks upload_with_locks;
    switch (upload_ctx.upload_kind) {
    case segment_upload_kind::non_compacted:
        upload_with_locks = co_await _policy.get_next_candidate(
          start_upload_offset,
          last_stable_offset,
          log,
          *_parent.get_offset_translator_state(),
          _conf->segment_upload_timeout);
        break;
    case segment_upload_kind::compacted:
        const auto& m = manifest();
        upload_with_locks = co_await _policy.get_next_compacted_segment(
          start_upload_offset, log, m, _conf->segment_upload_timeout);
        break;
    }

    auto upload = upload_with_locks.candidate;
    auto locks = std::move(upload_with_locks.read_locks);

    if (upload.sources.empty()) {
        vlog(
          _rtclog.debug,
          "upload candidate not found, start_upload_offset: {}, "
          "last_stable_offset: {}",
          start_upload_offset,
          last_stable_offset);
        // Indicate that the upload is not started
        co_return scheduled_upload{
          .result = std::nullopt,
          .inclusive_last_offset = {},
          .meta = std::nullopt,
          .name = std::nullopt,
          .delta = std::nullopt,
          .stop = ss::stop_iteration::yes,
        };
    }

    auto first_source = upload.sources.front();
    auto offset = upload.final_offset;
    auto base = upload.starting_offset;
    start_upload_offset = offset + model::offset(1);
    auto ot_state = _parent.get_offset_translator_state();
    auto delta = base - model::offset_cast(ot_state->from_log_offset(base));
    auto delta_offset_next = ot_state->next_offset_delta(upload.final_offset);

    // The upload is successful only if the segment, and tx_range are
    // uploaded.
    std::vector<ss::future<cloud_storage::upload_result>> all_uploads;

    all_uploads.emplace_back(upload_segment(upload, std::move(locks)));

    size_t tx_size = 0;
    // Compacted segments never have tx-manifests because all batches
    // generated by aborted transactions are removed during compaction.
    if (upload_ctx.upload_kind == segment_upload_kind::non_compacted) {
        const auto log_debug = [this]<typename... Args>(
                                 const fmt::format_string<Args...>& fmtstr,
                                 Args&&... args) {
            _rtclog.debug(fmtstr, std::forward<Args>(args)...);
        };
        const auto log_warn = [this]<typename... Args>(
                                const fmt::format_string<Args...>& fmtstr,
                                Args&&... args) {
            _rtclog.warn(fmtstr, std::forward<Args>(args)...);
        };
        auto handle_exception = [&all_uploads](auto logger) {
            vlog(
              logger,
              "Failed to get aborted transactions: {}",
              std::current_exception());
            for (auto& fut : all_uploads) {
                if (fut.failed()) {
                    vlog(logger, "Upload failed: {}", fut.get_exception());
                }
            }
        };
        try {
            auto tx = co_await get_aborted_transactions(upload);
            tx_size = tx.size();
            if (!tx.empty()) {
                all_uploads.emplace_back(upload_tx(upload, std::move(tx)));
            }
        } catch (const ss::gate_closed_exception&) {
            handle_exception(log_debug);
            throw;
        } catch (...) {
            handle_exception(log_warn);
            throw;
        }
    }

    auto upl_fut = aggregate_upload_results(std::move(all_uploads));

    auto is_compacted = first_source->is_compacted_segment()
                        && first_source->finished_self_compaction();
    co_return scheduled_upload{
      .result = std::move(upl_fut),
      .inclusive_last_offset = offset,
      .meta = cloud_storage::partition_manifest::segment_meta{
        .is_compacted = is_compacted,
        .size_bytes = upload.content_length,
        .base_offset = upload.starting_offset,
        .committed_offset = offset,
        .base_timestamp = upload.base_timestamp,
        .max_timestamp = upload.max_timestamp,
        .delta_offset = delta,
        .ntp_revision = _rev,
        .archiver_term = _start_term,
        .segment_term = upload.term,
        .delta_offset_end = delta_offset_next,
        .sname_format = cloud_storage::segment_name_format::v3,
        .metadata_size_hint = tx_size,
      },
      .name = upload.exposed_name, .delta = offset - base,
      .stop = ss::stop_iteration::no,
    };
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(model::offset last_stable_offset) {
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

    auto compacted_segments_upload_start = model::next_offset(
      manifest().get_last_uploaded_compacted_offset());

    std::vector<upload_context> params;

    params.emplace_back(
      segment_upload_kind::non_compacted,
      start_upload_offset,
      last_stable_offset,
      allow_reuploads_t::no);

    if (
      config::shard_local_cfg().cloud_storage_enable_compacted_topic_reupload()
      && _parent.get_ntp_config().is_compacted()) {
        params.emplace_back(
          segment_upload_kind::compacted,
          compacted_segments_upload_start,
          model::offset::max(),
          allow_reuploads_t::yes);
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
              ctx.last_offset,
              uploads_remaining);
            break;
        }

        vlog(
          _rtclog.debug,
          "scheduling uploads, start offset: {}, last offset: {}, upload kind: "
          "{}, uploads remaining: {}",
          ctx.start_offset,
          ctx.last_offset,
          ctx.upload_kind,
          uploads_remaining);

        // this metric is only relevant for non compacted uploads.
        if (ctx.upload_kind == segment_upload_kind::non_compacted) {
            _probe->upload_lag(ctx.last_offset - ctx.start_offset);
        }

        while (uploads_remaining > 0 && may_begin_uploads()) {
            auto should_stop = co_await ctx.schedule_single_upload(*this);
            if (should_stop == ss::stop_iteration::yes) {
                break;
            }

            // Decrement remaining upload count if the last call actually
            // scheduled an upload.
            if (!ctx.uploads.empty()) {
                const auto& last_scheduled = ctx.uploads.back();
                if (last_scheduled.result.has_value()) {
                    uploads_remaining -= 1;
                }
            }
        }

        auto upload_segments_count = std::count_if(
          ctx.uploads.begin(), ctx.uploads.end(), [](const auto& upload) {
              return upload.result.has_value();
          });
        vlog(
          _rtclog.debug,
          "scheduled {} uploads for upload kind: {}, uploads remaining: "
          "{}",
          upload_segments_count,
          ctx.upload_kind,
          uploads_remaining);

        scheduled_uploads.insert(
          scheduled_uploads.end(),
          std::make_move_iterator(ctx.uploads.begin()),
          std::make_move_iterator(ctx.uploads.end()));
    }

    co_return scheduled_uploads;
}

ss::future<ntp_archiver::upload_group_result> ntp_archiver::wait_uploads(
  std::vector<scheduled_upload> scheduled,
  segment_upload_kind segment_kind,
  bool inline_manifest) {
    ntp_archiver::upload_group_result total{};
    std::vector<ss::future<cloud_storage::upload_result>> flist;
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
        co_return total;
    }

    // Remember if we started with a clean STM: this will be used to decide
    // whether to maybe do an extra flush of manifest after upload, to get back
    // into a clean state.
    auto stm_was_clean = _parent.archival_meta_stm()->get_dirty(
                           _projected_manifest_clean_at)
                         == cluster::archival_metadata_stm::state_dirty::clean;

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
              return cloud_storage::upload_result::success;
          }));
    }

    auto segment_results = co_await ss::when_all_succeed(
      begin(flist), end(flist));

    if (upload_manifest_in_parallel) {
        // Drop the upload_result from manifest upload, we do not want to
        // count it in the subsequent success/failure counts.
        segment_results.pop_back();
    }

    if (!can_update_archival_metadata()) {
        // We exit early even if we have successfully uploaded some segments to
        // avoid interfering with an archiver that could have started on another
        // node.
        co_return upload_group_result{};
    }

    absl::flat_hash_map<cloud_storage::upload_result, size_t> upload_results;
    for (auto result : segment_results) {
        ++upload_results[result];
    }

    total.num_succeeded = upload_results[cloud_storage::upload_result::success];
    total.num_cancelled
      = upload_results[cloud_storage::upload_result::cancelled];
    total.num_failed = segment_results.size()
                       - (total.num_succeeded + total.num_cancelled);

    std::vector<cloud_storage::segment_meta> mdiff;
    for (size_t i = 0; i < segment_results.size(); i++) {
        if (segment_results[i] != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];

        if (segment_kind == segment_upload_kind::non_compacted) {
            _probe->uploaded(*upload.delta);
            _probe->uploaded_bytes(upload.meta->size_bytes);

            model::offset expected_base_offset;
            if (manifest().get_last_offset() < model::offset{0}) {
                expected_base_offset = model::offset{0};
            } else {
                expected_base_offset = manifest().get_last_offset()
                                       + model::offset{1};
            }
            if (upload.meta->base_offset > expected_base_offset) {
                _probe->gap_detected(
                  upload.meta->base_offset - expected_base_offset);
            }
        }

        mdiff.push_back(*upload.meta);
    }

    if (total.num_succeeded != 0) {
        vassert(
          _parent.archival_meta_stm(),
          "Archival metadata STM is not created for {} archiver",
          _ntp.path());

        auto deadline = ss::lowres_clock::now()
                        + _conf->manifest_upload_timeout;

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

        auto error = co_await _parent.archival_meta_stm()->add_segments(
          mdiff, manifest_clean_offset, deadline, _as);
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

ss::future<ntp_archiver::batch_result> ntp_archiver::wait_all_scheduled_uploads(
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
        wait_uploads(
          std::move(non_compacted_uploads),
          segment_upload_kind::non_compacted,
          inline_manifest_in_non_compacted_uploads),
        wait_uploads(
          std::move(compacted_uploads),
          segment_upload_kind::compacted,
          !inline_manifest_in_non_compacted_uploads));

    auto total_successful_uploads = non_compacted_result.num_succeeded
                                    + compacted_result.num_succeeded;
    if (total_successful_uploads > 0) {
        _last_segment_upload_time = ss::lowres_clock::now();
    }
    vlog(
      _rtclog.trace,
      "Segment uploads complete: {} successful uploads",
      total_successful_uploads);

    co_return batch_result{
      .non_compacted_upload_result = non_compacted_result,
      .compacted_upload_result = compacted_result};
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  std::optional<model::offset> lso_override) {
    vlog(_rtclog.debug, "Uploading next candidates called for {}", _ntp);
    auto last_stable_offset = lso_override ? *lso_override
                                           : _parent.last_stable_offset();
    ss::gate::holder holder(_gate);
    try {
        auto units = co_await ss::get_units(_mutex, 1, _as);
        auto scheduled_uploads = co_await schedule_uploads(last_stable_offset);
        co_return co_await wait_all_scheduled_uploads(
          std::move(scheduled_uploads));
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::abort_requested_exception&) {
    }
    co_return batch_result{
      .non_compacted_upload_result = {}, .compacted_upload_result = {}};
}

uint64_t ntp_archiver::estimate_backlog_size() {
    auto last_offset = manifest().size() ? manifest().get_last_offset()
                                         : model::offset(0);
    auto log_generic = _parent.log();
    auto log = dynamic_cast<storage::disk_log_impl*>(log_generic.get_impl());
    uint64_t total_size = std::accumulate(
      std::begin(log->segments()),
      std::end(log->segments()),
      0UL,
      [last_offset](
        uint64_t acc, const ss::lw_shared_ptr<storage::segment>& s) {
          if (s->offsets().dirty_offset > last_offset) {
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
    for (const auto& [key, meta] : m) {
        retry_chain_node fib(
          _conf->manifest_upload_timeout,
          _conf->upload_loop_initial_backoff,
          &rtc);
        auto sname = cloud_storage::generate_local_segment_name(
          meta.base_offset, meta.segment_term);
        auto spath = m.generate_segment_path(meta);
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
        retry_chain_node rc_node(
          _conf->manifest_upload_timeout,
          _conf->upload_loop_initial_backoff,
          &rtc);
        auto error = co_await _parent.archival_meta_stm()->truncate(
          adjusted_start_offset,
          ss::lowres_clock::now() + _conf->manifest_upload_timeout,
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
            co_await upload_manifest(sync_local_state_ctx_label);
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

ss::future<ss::stop_iteration>
ntp_archiver::upload_context::schedule_single_upload(ntp_archiver& archiver) {
    scheduled_upload f = co_await archiver.schedule_single_upload(*this);

    start_offset = f.inclusive_last_offset + model::offset(1);
    f.upload_kind = upload_kind;
    auto should_stop = f.stop;

    uploads.push_back(std::move(f));
    co_return should_stop;
}

ntp_archiver::upload_context::upload_context(
  segment_upload_kind upload_kind,
  model::offset start_offset,
  model::offset last_offset,
  allow_reuploads_t allow_reuploads)
  : upload_kind{upload_kind}
  , start_offset{start_offset}
  , last_offset{last_offset}
  , allow_reuploads{allow_reuploads}
  , uploads{} {}

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

ss::future<> ntp_archiver::housekeeping() {
    try {
        if (may_begin_uploads()) {
            // Acquire mutex to prevent concurrency between
            // external housekeeping jobs from upload_housekeeping_service
            // and retention/GC
            auto units = co_await ss::get_units(_mutex, 1, _as);
            co_await apply_retention();
            co_await garbage_collect();
        }
    } catch (std::exception& e) {
        vlog(_rtclog.warn, "Error occured during housekeeping", e.what());
    }
}

ss::future<> ntp_archiver::apply_retention() {
    if (!may_begin_uploads()) {
        co_return;
    }

    auto retention_calculator = retention_calculator::factory(
      manifest(), _parent.get_ntp_config());
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
        auto error = co_await _parent.archival_meta_stm()->truncate(
          *next_start_offset, deadline, _as);
        if (error != cluster::errc::success) {
            vlog(
              _rtclog.warn,
              "Failed to update archival metadata STM start offest according "
              "to retention policy: {}",
              error);
        }
    } else {
        vlog(
          _rtclog.debug,
          "{} Retention policies are already met.",
          retention_calculator->strategy_name());
    }
}

// Garbage collection can be improved as follows:
// * issue #6843: delete via DeleteObjects S3 api instead of deleting individual
// segments
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

    size_t successful_deletes{0};
    co_await ss::max_concurrent_for_each(
      to_remove,
      _concurrency,
      [this, &successful_deletes](
        const cloud_storage::partition_manifest::lw_segment_meta& meta) {
          auto path = manifest().generate_segment_path(meta);
          return ss::do_with(
            std::move(path), [this, &successful_deletes](auto& path) {
                return delete_segment(path).then(
                  [this, &successful_deletes, &path](
                    cloud_storage::upload_result res) {
                      if (res == cloud_storage::upload_result::success) {
                          ++successful_deletes;

                          vlog(
                            _rtclog.info,
                            "Deleted segment from cloud storage: {}",
                            path);
                      }
                  });
            });
      });

    const auto backlog_size_exceeded = to_remove.size()
                                       > _max_segments_pending_deletion();
    const auto all_deletes_succeeded = successful_deletes == to_remove.size();
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
        auto error = co_await _parent.archival_meta_stm()->cleanup_metadata(
          deadline, _as);

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.info,
              "Failed to clean up metadata after garbage collection: {}",
              error);
        }
    } else {
        vlog(
          _rtclog.info,
          "Failed to delete all selected segments from cloud storage. Will "
          "retry on the next housekeeping run.");
    }

    _probe->segments_deleted(static_cast<int64_t>(successful_deletes));
    vlog(
      _rtclog.debug, "Deleted {} segments from the cloud", successful_deletes);
}

ss::future<cloud_storage::upload_result>
ntp_archiver::delete_segment(const remote_segment_path& path) {
    _as.check();

    size_t entities_deleted = 3;
    retry_chain_node fib(
      _conf->manifest_upload_timeout * entities_deleted,
      _conf->cloud_storage_initial_backoff,
      &_rtcnode);

    auto res = co_await _remote.delete_object(
      get_bucket_name(), cloud_storage_clients::object_key{path}, fib);

    if (res == cloud_storage::upload_result::success) {
        if (auto delete_tx_res = co_await _remote.delete_object(
              _conf->bucket_name,
              cloud_storage_clients::object_key{
                cloud_storage::generate_remote_tx_path(path)},
              fib);
            delete_tx_res != cloud_storage::upload_result::success) {
            vlog(
              _rtclog.warn,
              "failed to delete transaction manifest: {}, result: {}",
              cloud_storage::generate_remote_tx_path(path),
              delete_tx_res);
        }

        if (auto delete_index_res = co_await _remote.delete_object(
              _conf->bucket_name,
              cloud_storage_clients::object_key{make_index_path(path)},
              fib);
            delete_index_res != cloud_storage::upload_result::success) {
            vlog(
              _rtclog.warn,
              "failed to delete index file: {}, result: {}",
              make_index_path(path),
              delete_index_res);
        }
    }

    co_return res;
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
    return res;
}

ss::future<std::optional<upload_candidate_with_locks>>
ntp_archiver::find_reupload_candidate(manifest_scanner_t scanner) {
    if (!may_begin_uploads()) {
        co_return std::nullopt;
    }
    auto run = scanner(_parent.start_offset(), manifest());
    if (!run.has_value()) {
        vlog(_rtclog.debug, "Scan didn't resulted in upload candidate");
        co_return std::nullopt;
    } else {
        vlog(_rtclog.debug, "Scan result: {}", run);
    }
    if (run->meta.base_offset >= _parent.start_offset()) {
        auto log_generic = _parent.log();
        auto& log = dynamic_cast<storage::disk_log_impl&>(
          *log_generic.get_impl());
        segment_collector collector(
          run->meta.base_offset,
          manifest(),
          log,
          run->meta.size_bytes,
          run->meta.committed_offset);
        collector.collect_segments(
          segment_collector_mode::collect_non_compacted);
        auto candidate = co_await collector.make_upload_candidate(
          _conf->upload_io_priority, _conf->segment_upload_timeout);
        if (candidate.candidate.exposed_name().empty()) {
            vlog(_rtclog.warn, "Failed to make upload candidate");
            co_return std::nullopt;
        }
        if (candidate.candidate.content_length != run->meta.size_bytes) {
            vlog(
              _rtclog.error,
              "Failed to make upload candidate with correct size, expected {}, "
              "actual {}",
              candidate.candidate,
              run->meta);
            co_return std::nullopt;
        }
        co_return candidate;
    }
    // segment_name exposed_name;
    upload_candidate candidate = {};
    candidate.starting_offset = run->meta.base_offset;
    candidate.content_length = run->meta.size_bytes;
    candidate.final_offset = run->meta.committed_offset;
    candidate.base_timestamp = run->meta.base_timestamp;
    candidate.max_timestamp = run->meta.max_timestamp;
    candidate.term = run->meta.segment_term;
    candidate.remote_sources = run->segments;
    // Reuploaded segment can only use new name format
    run->meta.sname_format = cloud_storage::segment_name_format::v3;
    candidate.exposed_name
      = cloud_storage::partition_manifest::generate_remote_segment_name(
        run->meta);
    // Create a remote upload candidate
    co_return upload_candidate_with_locks{std::move(candidate)};
}

ss::future<bool> ntp_archiver::upload(
  upload_candidate_with_locks upload_locks,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    if (upload_locks.candidate.sources.size() > 0) {
        return do_upload_local(std::move(upload_locks), source_rtc);
    }
    // Currently, the uploading of remote segments is disabled and
    // the only reason why the list of locks is empty is truncation.
    // The log could be truncated right after we scanned the manifest to
    // find upload candidate. In this case we will get an empty candidate
    // which is not a failure so we shuld return 'true'.
    return ss::make_ready_future<bool>(true);
}

ss::future<bool> ntp_archiver::do_upload_local(
  upload_candidate_with_locks upload_locks,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    if (!may_begin_uploads()) {
        co_return false;
    }
    auto [upload, locks] = std::move(upload_locks);

    for (const auto& s : upload.sources) {
        if (s->finished_self_compaction()) {
            vlog(
              _rtclog.warn,
              "Upload {} requested contains compacted segments.",
              upload.exposed_name);
            co_return false;
        }
    }

    if (upload.sources.empty()) {
        vlog(
          _rtclog.warn,
          "Upload of the {} requested but sources are empty",
          upload.exposed_name);
        co_return false;
    }

    auto units = co_await ss::get_units(_mutex, 1, _as);

    auto offset = upload.final_offset;
    auto base = upload.starting_offset;
    auto ot_state = _parent.get_offset_translator_state();
    auto delta = base - model::offset_cast(ot_state->from_log_offset(base));
    auto delta_offset_next = ot_state->next_offset_delta(upload.final_offset);

    // Upload segments and tx-manifest in parallel
    std::vector<ss::future<cloud_storage::upload_result>> futures;
    futures.emplace_back(upload_segment(upload, std::move(locks), source_rtc));

    size_t tx_size = 0;
    auto tx_range = co_await get_aborted_transactions(upload);
    if (!tx_range.empty()) {
        tx_size = tx_range.size();
        futures.emplace_back(
          upload_tx(upload, std::move(tx_range), source_rtc));
    }
    auto upl_res = co_await aggregate_upload_results(std::move(futures));

    if (upl_res != cloud_storage::upload_result::success) {
        vlog(
          _rtclog.warn,
          "Failed to upload segment: {}, error: {}",
          upload.exposed_name,
          upl_res);
        co_return false;
    }

    auto meta = cloud_storage::partition_manifest::segment_meta{
      .is_compacted = false,
      .size_bytes = upload.content_length,
      .base_offset = upload.starting_offset,
      .committed_offset = offset,
      .base_timestamp = upload.base_timestamp,
      .max_timestamp = upload.max_timestamp,
      .delta_offset = delta,
      .ntp_revision = _rev,
      .archiver_term = _start_term,
      .segment_term = upload.term,
      .delta_offset_end = delta_offset_next,
      .sname_format = cloud_storage::segment_name_format::v3,
      .metadata_size_hint = tx_size,
    };

    auto deadline = ss::lowres_clock::now() + _conf->manifest_upload_timeout;
    auto error = co_await _parent.archival_meta_stm()->add_segments(
      {meta}, std::nullopt, deadline);
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
    auto log_segment_size = config::shard_local_cfg().log_segment_size.value();
    const auto& cfg = _parent.raft()->log_config();
    if (cfg.has_overrides()) {
        log_segment_size = cfg.get_overrides().segment_size.value_or(
          log_segment_size);
    }
    return log_segment_size;
}

ss::future<bool>
ntp_archiver::prepare_transfer_leadership(ss::lowres_clock::duration timeout) {
    _paused = true;

    try {
        co_await ss::get_units(_uploads_active, 1, timeout);
        vlog(
          _rtclog.trace,
          "prepare_transfer_leadership: got units (current {})",
          _uploads_active.current());
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

void ntp_archiver::complete_transfer_leadership() {
    vlog(
      _rtclog.trace,
      "complete_transfer_leadership: current units (current {})",
      _uploads_active.current());
    _paused = false;
    _leader_cond.signal();
}

bool ntp_archiver::local_storage_pressure() const {
    auto eviction_offset = _parent.eviction_requested_offset();

    return eviction_offset.has_value()
           && _parent.archival_meta_stm()->get_last_clean_at()
                <= eviction_offset.value();
}

} // namespace archival
