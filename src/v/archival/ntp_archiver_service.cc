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

#include "archival/archival_policy.h"
#include "archival/logger.h"
#include "archival/retention_calculator.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/parser.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <exception>
#include <numeric>
#include <stdexcept>

namespace archival {

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp,
  cluster::partition_manager& partition_manager,
  const configuration& conf,
  cloud_storage::remote& remote,
  ss::lw_shared_ptr<cluster::partition> part)
  : _probe(conf.ntp_metrics_disabled, ntp.ntp())
  , _ntp(ntp.ntp())
  , _rev(ntp.get_initial_revision())
  , _partition_manager(partition_manager)
  , _remote(remote)
  , _partition(std::move(part))
  , _policy(_ntp, conf.time_limit, conf.upload_io_priority)
  , _bucket(conf.bucket_name)
  , _gate()
  , _rtcnode(_as)
  , _rtclog(archival_log, _rtcnode, _ntp.path())
  , _cloud_storage_initial_backoff(conf.cloud_storage_initial_backoff)
  , _segment_upload_timeout(conf.segment_upload_timeout)
  , _metadata_sync_timeout(conf.manifest_upload_timeout)
  , _upload_loop_initial_backoff(conf.upload_loop_initial_backoff)
  , _upload_loop_max_backoff(conf.upload_loop_max_backoff)
  , _sync_manifest_timeout(
      config::shard_local_cfg()
        .cloud_storage_readreplica_manifest_sync_timeout_ms.bind())
  , _upload_sg(conf.upload_scheduling_group)
  , _io_priority(conf.upload_io_priority)
  , _housekeeping_interval(
      config::shard_local_cfg().cloud_storage_housekeeping_interval_ms.bind())
  , _housekeeping_jitter(_housekeeping_interval(), 5ms)
  , _next_housekeeping(_housekeeping_jitter()) {
    vassert(
      _partition && _partition->is_elected_leader(),
      "must be the leader to launch ntp_archiver {}",
      _ntp);
    _start_term = _partition->term();
    // Override bucket for read-replica
    if (_partition && _partition->is_read_replica_mode_enabled()) {
        _bucket = _partition->get_read_replica_bucket();
    }

    vlog(
      archival_log.debug,
      "created ntp_archiver {} in term {}",
      _ntp,
      _start_term);
}

const cloud_storage::partition_manifest& ntp_archiver::manifest() const {
    vassert(_partition, "Partition {} is not available", _ntp.path());
    vassert(
      _partition->archival_meta_stm(),
      "Archival STM is not available for {}",
      _ntp.path());
    return _partition->archival_meta_stm()->manifest();
}

void ntp_archiver::run_sync_manifest_loop() {
    vassert(
      _upload_loop_state == loop_state::initial,
      "attempt to start manifest sync loop for {} when upload loop has been "
      "active",
      _ntp);
    vassert(
      _sync_manifest_loop_state != loop_state::started,
      "sync manifest loop for ntp {} already started",
      _ntp);
    _sync_manifest_loop_state = loop_state::started;

    // NOTE: not using ssx::spawn_with_gate_then here because we want to log
    // inside the gate (so that _rtclog is guaranteed to be alive).
    ssx::spawn_with_gate(_gate, [this] {
        return sync_manifest_loop()
          .handle_exception_type([](const ss::abort_requested_exception&) {})
          .handle_exception_type([](const ss::sleep_aborted&) {})
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([this](const ss::semaphore_timed_out& e) {
              vlog(
                _rtclog.warn,
                "Semaphore timed out in sync manifest loop: {}. This may be "
                "due to the system being overloaded. The loop will restart.",
                e);
          })
          .handle_exception([this](std::exception_ptr e) {
              vlog(_rtclog.error, "sync manifest loop error: {}", e);
          })
          .finally([this] {
              vlog(_rtclog.debug, "sync manifest loop stopped");
              _sync_manifest_loop_state = loop_state::stopped;
          });
    });
}

void ntp_archiver::run_upload_loop() {
    vassert(
      _sync_manifest_loop_state == loop_state::initial,
      "attempt to start upload loop for {} when manifest sync loop has been "
      "active",
      _ntp);
    vassert(
      _upload_loop_state != loop_state::started,
      "upload loop for ntp {} already started",
      _ntp);
    _upload_loop_state = loop_state::started;

    // NOTE: not using ssx::spawn_with_gate_then here because we want to log
    // inside the gate (so that _rtclog is guaranteed to be alive).
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(
                 _upload_sg, [this] { return upload_loop(); })
          .handle_exception_type([](const ss::abort_requested_exception&) {})
          .handle_exception_type([](const ss::sleep_aborted&) {})
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([this](const ss::semaphore_timed_out& e) {
              vlog(
                _rtclog.warn,
                "Semaphore timed out in the upload loop: {}. This may be "
                "due to the system being overloaded. The loop will restart.",
                e);
          })
          .handle_exception([this](std::exception_ptr e) {
              vlog(_rtclog.error, "upload loop error: {}", e);
          })
          .finally([this] {
              vlog(_rtclog.debug, "upload loop stopped");
              _upload_loop_state = loop_state::stopped;
          });
    });
}

ss::future<> ntp_archiver::upload_loop() {
    ss::lowres_clock::duration backoff = _upload_loop_initial_backoff;

    while (upload_loop_can_continue()) {
        // Bump up archival STM's state to make sure that it's not lagging
        // behind too far. If the STM is lagging behind we will have to read a
        // lot of data next time we upload something.
        vassert(
          _partition,
          "Upload loop: partition is not set for {} archiver",
          _ntp.path());
        vassert(
          _partition->archival_meta_stm(),
          "Upload loop: archival metadata STM is not created for {} archiver",
          _ntp.path());

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        co_await _partition->archival_meta_stm()->sync(sync_timeout);

        auto [non_compacted_upload_result, compacted_upload_result]
          = co_await upload_next_candidates();
        if (non_compacted_upload_result.num_failed != 0) {
            // The logic in class `remote` already does retries: if we get here,
            // it means the upload failed after several retries, indicating
            // something non-transient may be wrong.  Hence error severity.
            vlog(
              _rtclog.error,
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

        if (!upload_loop_can_continue()) {
            break;
        }

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
            backoff = std::min(backoff * 2, _upload_loop_max_backoff);
        } else {
            backoff = _upload_loop_initial_backoff;
        }
    }
}

ss::future<> ntp_archiver::sync_manifest_loop() {
    while (sync_manifest_loop_can_continue()) {
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
          _rtclog.debug, "Updating the archival_meta_stm in read-replica mode");
        std::vector<cloud_storage::segment_meta> mdiff;
        auto offset
          = _partition->archival_meta_stm()->manifest().get_last_offset()
            + model::offset(1);
        // TODO: this code needs to be updated when the compacted segment
        // uploads are merged.
        for (auto it = m.segment_containing(offset); it != m.end(); it++) {
            mdiff.push_back(it->second);
        }
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto error = co_await _partition->archival_meta_stm()->add_segments(
          mdiff, deadline, _as);
        if (
          error != cluster::errc::success
          && error != cluster::errc::not_leader) {
            vlog(
              _rtclog.warn, "archival metadata STM update failed: {}", error);
        }
        auto last_offset = manifest().get_last_offset();
        vlog(_rtclog.debug, "manifest last_offset: {}", last_offset);
        co_return cloud_storage::download_result::success;
    }
    __builtin_unreachable();
}

bool ntp_archiver::upload_loop_can_continue() const {
    return !_as.abort_requested() && !_gate.is_closed()
           && _partition->is_elected_leader()
           && _partition->term() == _start_term;
}

bool ntp_archiver::sync_manifest_loop_can_continue() const {
    // todo: think about it
    return !_as.abort_requested() && !_gate.is_closed()
           && _partition->is_elected_leader()
           && _partition->term() == _start_term;
}

bool ntp_archiver::housekeeping_can_continue() const {
    return !_as.abort_requested() && !_gate.is_closed()
           && _partition->is_elected_leader()
           && _partition->term() == _start_term;
}

ss::future<> ntp_archiver::stop() {
    _as.request_abort();
    return _gate.close();
}

const model::ntp& ntp_archiver::get_ntp() const { return _ntp; }

model::initial_revision_id ntp_archiver::get_revision_id() const {
    return _rev;
}

const ss::lowres_clock::time_point ntp_archiver::get_last_upload_time() const {
    return _last_upload_time;
}

ss::future<
  std::pair<cloud_storage::partition_manifest, cloud_storage::download_result>>
ntp_archiver::download_manifest() {
    gate_guard guard{_gate};
    retry_chain_node fib(
      _metadata_sync_timeout, _cloud_storage_initial_backoff, &_rtcnode);
    cloud_storage::partition_manifest tmp(_ntp, _rev);
    auto path = tmp.get_manifest_path();
    auto key = cloud_storage::remote_manifest_path(
      std::filesystem::path(std::move(path)));
    vlog(_rtclog.debug, "Downloading manifest");
    auto result = co_await _remote.download_manifest(_bucket, key, tmp, fib);

    // It's OK if the manifest is not found for a newly created topic. The
    // condition in if statement is not guaranteed to cover all cases for new
    // topics, so false positives may happen for this warn.
    if (
      result == cloud_storage::download_result::notfound
      && _partition->high_watermark() != model::offset(0)
      && _partition->term() != model::term_id(1)) {
        vlog(
          _rtclog.warn,
          "Manifest for {} not found in S3, partition high_watermark: {}, "
          "partition term: {}",
          _ntp,
          _partition->high_watermark(),
          _partition->term());
    }

    co_return std::make_pair(tmp, result);
}

ss::future<cloud_storage::upload_result> ntp_archiver::upload_manifest() {
    gate_guard guard{_gate};
    retry_chain_node fib(
      _metadata_sync_timeout, _cloud_storage_initial_backoff, &_rtcnode);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    vlog(
      ctxlog.debug,
      "Uploading manifest, path: {}",
      manifest().get_manifest_path());
    co_return co_await _remote.upload_manifest(_bucket, manifest(), fib);
}

remote_segment_path
ntp_archiver::segment_path_for_candidate(const upload_candidate& candidate) {
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
      .sname_format = cloud_storage::segment_name_format::v2,
    };

    return manifest().generate_segment_path(val);
}

// from offset to offset (by record batch boundary)
ss::future<cloud_storage::upload_result>
ntp_archiver::upload_segment(upload_candidate candidate) {
    gate_guard guard{_gate};
    retry_chain_node fib(
      _segment_upload_timeout, _cloud_storage_initial_backoff, &_rtcnode);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    auto path = segment_path_for_candidate(candidate);
    vlog(ctxlog.debug, "Uploading segment {} to {}", candidate, path);

    auto original_term = _partition->term();
    auto lazy_abort_source = cloud_storage::lazy_abort_source{
      "lost leadership or term changed during upload, "
      "current leadership status: {}, "
      "current term: {}, "
      "original term: {}",
      [this, original_term](cloud_storage::lazy_abort_source& las) {
          auto lost_leadership = !_partition->is_elected_leader()
                                 || _partition->term() != original_term;
          if (unlikely(lost_leadership)) {
              std::string reason{las.abort_reason()};
              las.abort_reason(fmt::format(
                fmt::runtime(reason),
                _partition->is_elected_leader(),
                _partition->term(),
                original_term));
          }
          return lost_leadership;
      },
    };

    auto reset_func =
      [this,
       candidate]() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        co_return std::make_unique<storage::concat_segment_reader_view>(
          candidate.sources,
          candidate.file_offset,
          candidate.final_file_offset,
          _io_priority);
    };

    co_return co_await _remote.upload_segment(
      _bucket,
      path,
      candidate.content_length,
      reset_func,
      fib,
      lazy_abort_source);
}

ss::future<cloud_storage::upload_result>
ntp_archiver::upload_tx(upload_candidate candidate) {
    gate_guard guard{_gate};
    retry_chain_node fib(
      _segment_upload_timeout, _cloud_storage_initial_backoff, &_rtcnode);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    vlog(
      ctxlog.debug, "Uploading segment's tx range {}", candidate.exposed_name);

    auto tx_range = co_await _partition->aborted_transactions(
      candidate.starting_offset, candidate.final_offset);

    if (tx_range.empty()) {
        // The actual upload only happens if tx_range is not empty.
        // The remote_segment should act as if the tx_range is empty if the
        // request returned NoSuchKey error.
        co_return cloud_storage::upload_result::success;
    }

    auto path = segment_path_for_candidate(candidate);

    cloud_storage::tx_range_manifest manifest(path, tx_range);

    co_return co_await _remote.upload_manifest(_bucket, manifest, fib);
}

ss::future<ntp_archiver::scheduled_upload>
ntp_archiver::schedule_single_upload(const upload_context& upload_ctx) {
    auto start_upload_offset = upload_ctx.start_offset;
    auto last_stable_offset = upload_ctx.last_offset;
    std::optional<storage::log> log = _partition_manager.log(_ntp);
    if (!log) {
        vlog(_rtclog.warn, "couldn't find log in log manager");
        co_return scheduled_upload{.stop = ss::stop_iteration::yes};
    }

    upload_candidate_with_locks upload_with_locks;
    switch (upload_ctx.upload_kind) {
    case segment_upload_kind::non_compacted:
        upload_with_locks = co_await _policy.get_next_candidate(
          start_upload_offset,
          last_stable_offset,
          *log,
          *_partition->get_offset_translator_state(),
          _segment_upload_timeout);
        break;
    case segment_upload_kind::compacted:
        const auto& m = manifest();
        upload_with_locks = co_await _policy.get_next_compacted_segment(
          start_upload_offset, *log, m, _segment_upload_timeout);
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
          .segment_read_locks = {},
        };
    }

    auto first_source = upload.sources.front();
    if (
      manifest().contains(upload.exposed_name) && !upload_ctx.allow_reuploads) {
        // If the manifest already contains the name we have the following
        // cases
        //
        // manifest: [A-B], upload: [C-D] where A/C are base offsets and B/D
        // are committed offsets
        // invariant:
        // - A == C (because the name contains base offset)
        // cases:
        // - B < D:
        //   - We need to upload the segment since it has more data.
        //     Skipping the upload is not an option since partial upload
        //     is not guaranteed to start from an offset which is not equal
        //     to B (which will trigger a loop).
        // - B > D:
        //   - Normally this shouldn't happen because we will lookup
        //     offset B to start the next upload and the segment returned by
        //     the policy will have commited offset which is less than this
        //     value. We need to log a warning and continue with the largest
        //     offset.
        // - B == D:
        //   - Same as previoius. We need to log error and continue with the
        //   largest offset.
        const auto& meta = manifest().get(upload.exposed_name);
        auto dirty_offset = first_source->offsets().dirty_offset;
        if (meta->committed_offset < dirty_offset) {
            vlog(
              _rtclog.info,
              "will re-upload {}, last offset in the manifest {}, "
              "candidate dirty offset {}",
              upload,
              meta->committed_offset,
              dirty_offset);
        } else if (meta->committed_offset >= dirty_offset) {
            vlog(
              _rtclog.warn,
              "skip upload {} because it's already in the manifest, "
              "last offset in the manifest {}, candidate dirty offset {}",
              upload,
              meta->committed_offset,
              dirty_offset);
            start_upload_offset = meta->committed_offset;
            co_return scheduled_upload{
              .result = std::nullopt,
              .inclusive_last_offset = start_upload_offset,
              .meta = std::nullopt,
              .name = std::nullopt,
              .delta = std::nullopt,
              .stop = ss::stop_iteration::no,
              .segment_read_locks = {},
            };
        }
    }
    auto offset = upload.final_offset;
    auto base = upload.starting_offset;
    start_upload_offset = offset + model::offset(1);
    auto ot_state = _partition->get_offset_translator_state();
    auto delta = base - model::offset_cast(ot_state->from_log_offset(base));
    auto delta_offset_end = upload.final_offset
                            - model::offset_cast(
                              ot_state->from_log_offset(upload.final_offset));

    // The upload is successful only if both segment and tx_range are uploaded.
    auto upl_fut
      = ss::when_all(upload_segment(upload), upload_tx(upload))
          .then([](auto tup) {
              auto [fs, ftx] = std::move(tup);
              auto rs = fs.get();
              auto rtx = ftx.get();
              if (
                rs == cloud_storage::upload_result::success
                && rtx == cloud_storage::upload_result::success) {
                  return rs;
              } else if (rs != cloud_storage::upload_result::success) {
                  return rs;
              }
              return rtx;
          });
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
        .delta_offset_end = delta_offset_end,
        .sname_format = cloud_storage::segment_name_format::v2,
      },
      .name = upload.exposed_name, .delta = offset - base,
      .stop = ss::stop_iteration::no,
      .segment_read_locks = std::move(locks),
    };
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(model::offset last_stable_offset) {
    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains dirty_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if there is no segments.
    auto start_upload_offset = manifest().size() ? manifest().get_last_offset()
                                                     + model::offset(1)
                                                 : model::offset(0);

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
      && _partition->get_ntp_config().is_compacted()) {
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
            _probe.upload_lag(ctx.last_offset - ctx.start_offset);
        }

        co_await ss::repeat(
          [this, &ctx, &uploads_remaining]() -> ss::future<ss::stop_iteration> {
              if (uploads_remaining <= 0 || !upload_loop_can_continue()) {
                  co_return ss::stop_iteration::yes;
              }

              auto should_stop = co_await ctx.schedule_single_upload(*this);

              // At the end of each context is an upload with should_stop=yes,
              // which does not upload anything but signals the end of the
              // uploads. Do not decrement remaining uploads for this type of
              // return value. When should_stop=no, we have a real upload and
              // decrement the counter.
              if (should_stop == ss::stop_iteration::no) {
                  uploads_remaining -= 1;
              }
              co_return should_stop;
          });

        vlog(
          _rtclog.debug,
          "scheduled {} uploads for upload kind: {}, uploads remaining: {}",
          ctx.uploads.size(),
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
  std::vector<scheduled_upload> scheduled, segment_upload_kind segment_kind) {
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
        vlog(_rtclog.debug, "no uploads started, returning");
        co_return total;
    }
    auto results = co_await ss::when_all_succeed(begin(flist), end(flist));

    if (!upload_loop_can_continue()) {
        // We exit early even if we have successfully uploaded some segments to
        // avoid interfering with an archiver that could have started on another
        // node.
        co_return upload_group_result{};
    }

    absl::flat_hash_map<cloud_storage::upload_result, size_t> upload_results;
    for (auto result : results) {
        ++upload_results[result];
    }

    total.num_succeeded = upload_results[cloud_storage::upload_result::success];
    total.num_cancelled
      = upload_results[cloud_storage::upload_result::cancelled];
    total.num_failed = results.size()
                       - (total.num_succeeded + total.num_cancelled);

    std::vector<cloud_storage::segment_meta> mdiff;
    for (size_t i = 0; i < results.size(); i++) {
        if (results[i] != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];

        if (segment_kind == segment_upload_kind::non_compacted) {
            _probe.uploaded(*upload.delta);
            _probe.uploaded_bytes(upload.meta->size_bytes);

            model::offset expected_base_offset;
            if (manifest().get_last_offset() < model::offset{0}) {
                expected_base_offset = model::offset{0};
            } else {
                expected_base_offset = manifest().get_last_offset()
                                       + model::offset{1};
            }
            if (upload.meta->base_offset > expected_base_offset) {
                _probe.gap_detected(
                  upload.meta->base_offset - expected_base_offset);
            }
        }

        mdiff.push_back(*upload.meta);
    }

    if (total.num_succeeded != 0) {
        vassert(
          _partition, "Partition is not set for {} archiver", _ntp.path());
        vassert(
          _partition->archival_meta_stm(),
          "Archival metadata STM is not created for {} archiver",
          _ntp.path());
        auto deadline = ss::lowres_clock::now() + _metadata_sync_timeout;
        auto error = co_await _partition->archival_meta_stm()->add_segments(
          mdiff, deadline, _as);
        if (
          error != cluster::errc::success
          && error != cluster::errc::not_leader) {
            vlog(
              _rtclog.warn, "archival metadata STM update failed: {}", error);
        }

        vlog(
          _rtclog.debug,
          "successfully uploaded {} segments (failed {} uploads)",
          total.num_succeeded,
          total.num_failed);
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

    auto [non_compacted_result, compacted_result]
      = co_await ss::when_all_succeed(
        wait_uploads(
          std::move(non_compacted_uploads), segment_upload_kind::non_compacted),
        wait_uploads(
          std::move(compacted_uploads), segment_upload_kind::compacted));

    auto total_successful_uploads = non_compacted_result.num_succeeded
                                    + compacted_result.num_succeeded;
    if (total_successful_uploads != 0) {
        vlog(
          _rtclog.debug,
          "total successful uploads: {}, re-uploading manifest file",
          total_successful_uploads);
        if (auto res = co_await upload_manifest();
            res != cloud_storage::upload_result::success) {
            vlog(
              _rtclog.warn,
              "manifest upload to {} failed",
              manifest().get_manifest_path());
        }

        _last_upload_time = ss::lowres_clock::now();
    }

    co_return batch_result{
      .non_compacted_upload_result = non_compacted_result,
      .compacted_upload_result = compacted_result};
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  std::optional<model::offset> lso_override) {
    vlog(_rtclog.debug, "Uploading next candidates called for {}", _ntp);
    auto last_stable_offset = lso_override ? *lso_override
                                           : _partition->last_stable_offset();
    return ss::with_gate(
             _gate,
             [this, last_stable_offset] {
                 return ss::with_semaphore(
                   _mutex,
                   1,
                   [this, last_stable_offset]() -> ss::future<batch_result> {
                       auto scheduled_uploads = co_await schedule_uploads(
                         last_stable_offset);
                       co_return co_await wait_all_scheduled_uploads(
                         std::move(scheduled_uploads));
                   });
             })
      .handle_exception_type([](const ss::gate_closed_exception&) {
          return ss::make_ready_future<batch_result>(batch_result{
            .non_compacted_upload_result = {}, .compacted_upload_result = {}});
      })
      .handle_exception_type([](const ss::abort_requested_exception&) {
          return ss::make_ready_future<batch_result>(batch_result{
            .non_compacted_upload_result = {}, .compacted_upload_result = {}});
      });
}

uint64_t ntp_archiver::estimate_backlog_size() {
    auto last_offset = manifest().size() ? manifest().get_last_offset()
                                         : model::offset(0);
    auto opt_log = _partition_manager.log(_ntp);
    if (!opt_log) {
        return 0U;
    }
    auto log = dynamic_cast<storage::disk_log_impl*>(opt_log->get_impl());
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
ntp_archiver::maybe_truncate_manifest(retry_chain_node& rtc) {
    ss::gate::holder gh(_gate);
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    vlog(ctxlog.info, "archival metadata cleanup started");
    model::offset adjusted_start_offset = model::offset::min();
    const auto& m = manifest();
    for (const auto& [key, meta] : m) {
        retry_chain_node fib(
          _metadata_sync_timeout, _upload_loop_initial_backoff, &rtc);
        auto sname = cloud_storage::generate_local_segment_name(
          meta.base_offset, meta.segment_term);
        auto spath = m.generate_segment_path(meta);
        auto result = co_await _remote.segment_exists(_bucket, spath, fib);
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
      && _partition->archival_meta_stm()) {
        vlog(
          ctxlog.info,
          "archival metadata cleanup, some segments will be removed from the "
          "manifest, start offset before cleanup: {}",
          manifest().get_start_offset());
        retry_chain_node rc_node(
          _metadata_sync_timeout, _upload_loop_initial_backoff, &rtc);
        auto error = co_await _partition->archival_meta_stm()->truncate(
          adjusted_start_offset,
          ss::lowres_clock::now() + _metadata_sync_timeout,
          _as);
        if (error != cluster::errc::success) {
            vlog(ctxlog.warn, "archival metadata STM update failed: {}", error);
            throw std::system_error(error);
        } else {
            vlog(
              ctxlog.debug,
              "archival metadata STM update passed, re-uploading manifest");
            co_await upload_manifest();
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
        if (housekeeping_can_continue()) {
            co_await apply_retention();
            co_await garbage_collect();
        }

        if (housekeeping_can_continue()) {
            co_await upload_manifest();
        }
    } catch (std::exception& e) {
        vlog(_rtclog.warn, "Error occured during housekeeping", e.what());
    }
}

ss::future<> ntp_archiver::apply_retention() {
    if (!housekeeping_can_continue()) {
        co_return;
    }

    auto retention_calculator = retention_calculator::factory(
      manifest(), _partition->get_ntp_config());
    if (!retention_calculator) {
        co_return;
    }

    auto next_start_offset = retention_calculator->next_start_offset();
    if (next_start_offset) {
        vlog(
          _rtclog.debug,
          "{} Advancing start offset to {} satisfy retention policy",
          retention_calculator->strategy_name(),
          *next_start_offset);

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto error = co_await _partition->archival_meta_stm()->truncate(
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
// * issue #6844: flush _replaced and advance the start offset even if the
// deletions fail if the backlog breaches a certain limit. This can lead to
// segments begin orphaned, but it's preferable to unbounded backlog growth.
ss::future<> ntp_archiver::garbage_collect() {
    if (!housekeeping_can_continue()) {
        co_return;
    }

    auto to_remove = _partition->archival_meta_stm()->get_segments_to_cleanup();

    std::atomic<size_t> successful_deletes{0};
    co_await ss::max_concurrent_for_each(
      to_remove,
      _concurrency,
      [this, &successful_deletes](const cloud_storage::segment_meta& meta) {
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

    if (successful_deletes == to_remove.size()) {
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto error = co_await _partition->archival_meta_stm()->cleanup_metadata(
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

    _probe.segments_deleted(successful_deletes);
    vlog(
      _rtclog.debug, "Deleted {} segments from the cloud", successful_deletes);
}

ss::future<cloud_storage::upload_result>
ntp_archiver::delete_segment(const remote_segment_path& path) {
    _as.check();

    retry_chain_node fib(
      _metadata_sync_timeout, _cloud_storage_initial_backoff, &_rtcnode);

    auto res = co_await _remote.delete_object(
      _bucket, s3::object_key{path}, fib);

    if (res == cloud_storage::upload_result::success) {
        auto tx_range_manifest_path
          = cloud_storage::tx_range_manifest(path).get_manifest_path();
        co_await _remote.delete_object(
          _bucket, s3::object_key{tx_range_manifest_path}, fib);
    }

    co_return res;
}

} // namespace archival
