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
  , _io_priority(conf.upload_io_priority) {
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

        auto result = co_await upload_next_candidates();
        if (result.num_failed != 0) {
            // The logic in class `remote` already does retries: if we get here,
            // it means the upload failed after several retries, justifying
            // a warning to the operator: something non-transient may be wrong,
            // although we do also see this in practice on AWS S3 occasionally
            // during normal operation.
            vlog(
              _rtclog.warn,
              "Failed to upload {} segments out of {}",
              result.num_failed,
              result.num_succeded + result.num_failed + result.num_cancelled);
        } else if (result.num_succeded != 0) {
            vlog(
              _rtclog.debug,
              "Successfuly uploaded {} segments",
              result.num_succeded);
        }

        if (result.num_cancelled != 0) {
            vlog(
              _rtclog.debug,
              "Cancelled upload of {} segments",
              result.num_cancelled);
        }

        if (!upload_loop_can_continue()) {
            break;
        }

        if (result.num_succeded == 0) {
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
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto error = co_await _partition->archival_meta_stm()->add_segments(
          m, deadline, _as);
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

// from offset to offset (by record batch boundary)
ss::future<cloud_storage::upload_result>
ntp_archiver::upload_segment(upload_candidate candidate) {
    gate_guard guard{_gate};
    retry_chain_node fib(
      _segment_upload_timeout, _cloud_storage_initial_backoff, &_rtcnode);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());

    auto path = cloud_storage::generate_remote_segment_path(
      _ntp, _rev, candidate.exposed_name, _start_term);

    vlog(ctxlog.debug, "Uploading segment {} to {}", candidate, path);

    auto reset_func = [this, candidate] {
        return candidate.source->reader().data_stream(
          candidate.file_offset, candidate.final_file_offset, _io_priority);
    };

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

    auto path = cloud_storage::generate_remote_segment_path(
      _ntp, _rev, candidate.exposed_name, _start_term);

    cloud_storage::tx_range_manifest manifest(path, tx_range);

    co_return co_await _remote.upload_manifest(_bucket, manifest, fib);
}

ss::future<ntp_archiver::scheduled_upload> ntp_archiver::schedule_single_upload(
  model::offset start_upload_offset, model::offset last_stable_offset) {
    std::optional<storage::log> log = _partition_manager.log(_ntp);
    if (!log) {
        vlog(_rtclog.warn, "couldn't find log in log manager");
        co_return scheduled_upload{.stop = ss::stop_iteration::yes};
    }

    auto upload = co_await _policy.get_next_candidate(
      start_upload_offset,
      last_stable_offset,
      *log,
      *_partition->get_offset_translator_state());

    if (upload.source.get() == nullptr) {
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
          .segment_read_lock = std::nullopt,
        };
    }
    if (manifest().contains(upload.exposed_name)) {
        // If the manifest already contains the name we have the following
        // cases
        //
        // manifest: [A-B], upload: [C-D] where A/C are base offsets and B/D
        // are committed offsets
        // invariant:
        // - A == C (because the name contains base offset)
        // cases:
        // - B < C:
        //   - We need to upload the segment since it has more data.
        //     Skipping the upload is not an option since partial upload
        //     is not guaranteed to start from an offset which is not equal
        //     to B (which will trigger a loop).
        // - B > C:
        //   - Normally this shouldn't happen because we will lookup
        //     offset B to start the next upload and the segment returned by
        //     the policy will have commited offset which is less than this
        //     value. We need to log a warning and continue with the largest
        //     offset.
        // - B == C:
        //   - Same as previoius. We need to log error and continue with the
        //   largest offset.
        const auto& meta = manifest().get(upload.exposed_name);
        auto dirty_offset = upload.source->offsets().dirty_offset;
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
              .segment_read_lock = std::nullopt,
            };
        }
    }
    auto offset = upload.final_offset;
    auto base = upload.starting_offset;
    start_upload_offset = offset + model::offset(1);
    auto delta
      = base - _partition->get_offset_translator_state()->from_log_offset(base);

    auto segment_lock_deadline = std::chrono::steady_clock::now()
                                 + _segment_upload_timeout;
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
    co_return scheduled_upload{
      .result = std::move(upl_fut),
      .inclusive_last_offset = offset,
      .meta = cloud_storage::partition_manifest::segment_meta{
        .is_compacted = upload.source->is_compacted_segment(),
        .size_bytes = upload.content_length,
        .base_offset = upload.starting_offset,
        .committed_offset = offset,
        .base_timestamp = upload.base_timestamp,
        .max_timestamp = upload.max_timestamp,
        .delta_offset = delta,
        .ntp_revision = _rev,
        .archiver_term = _start_term,
      },
      .name = upload.exposed_name, .delta = offset - base,
      .stop = ss::stop_iteration::no,
      .segment_read_lock = co_await upload.source->read_lock(segment_lock_deadline),
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

    vlog(
      _rtclog.debug,
      "scheduling uploads, start_upload_offset: {}, last_stable_offset: {}",
      start_upload_offset,
      last_stable_offset);
    _probe.upload_lag(last_stable_offset - start_upload_offset);

    return ss::do_with(
      std::vector<scheduled_upload>(),
      size_t(0),
      start_upload_offset,
      [this, last_stable_offset](
        std::vector<scheduled_upload>& scheduled,
        size_t& ix,
        model::offset& start_upload_offset) {
          return ss::repeat([this,
                             &start_upload_offset,
                             last_stable_offset,
                             &scheduled,
                             &ix] {
                     if (ix == _concurrency) {
                         return ss::make_ready_future<ss::stop_iteration>(
                           ss::stop_iteration::yes);
                     }

                     if (!upload_loop_can_continue()) {
                         return ss::make_ready_future<ss::stop_iteration>(
                           ss::stop_iteration::yes);
                     }

                     ++ix;
                     return schedule_single_upload(
                              start_upload_offset, last_stable_offset)
                       .then([&scheduled,
                              &start_upload_offset](scheduled_upload upload) {
                           auto res = upload.stop;
                           start_upload_offset = upload.inclusive_last_offset
                                                 + model::offset(1);
                           scheduled.push_back(std::move(upload));
                           return ss::make_ready_future<ss::stop_iteration>(
                             res);
                       });
                 })
            .then([&scheduled] {
                return ss::make_ready_future<std::vector<scheduled_upload>>(
                  std::move(scheduled));
            });
      });
}

ss::future<ntp_archiver::batch_result> ntp_archiver::wait_all_scheduled_uploads(
  std::vector<ntp_archiver::scheduled_upload> scheduled) {
    ntp_archiver::batch_result total{};
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
        co_return batch_result{};
    }

    absl::flat_hash_map<cloud_storage::upload_result, size_t> upload_results;
    for (auto result : results) {
        ++upload_results[result];
    }

    total.num_succeded = upload_results[cloud_storage::upload_result::success];
    total.num_cancelled
      = upload_results[cloud_storage::upload_result::cancelled];
    total.num_failed = results.size()
                       - (total.num_succeded + total.num_cancelled);

    cloud_storage::partition_manifest mdiff(_ntp, _rev);
    for (size_t i = 0; i < results.size(); i++) {
        if (results[i] != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];

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

        mdiff.add(segment_name(*upload.name), *upload.meta);
    }

    if (total.num_succeded != 0) {
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
          "successfully uploaded {} segments (failed {} uploads), "
          "re-uploading manifest file",
          total.num_succeded,
          total.num_failed);

        auto res = co_await upload_manifest();
        if (res != cloud_storage::upload_result::success) {
            vlog(
              _rtclog.warn,
              "manifest upload to {} failed",
              manifest().get_manifest_path());
        }

        _last_upload_time = ss::lowres_clock::now();
    }
    co_return total;
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
                   _mutex, 1, [this, last_stable_offset] {
                       return schedule_uploads(last_stable_offset)
                         .then([this](std::vector<scheduled_upload> scheduled) {
                             return wait_all_scheduled_uploads(
                               std::move(scheduled));
                         });
                   });
             })
      .handle_exception_type([](const ss::gate_closed_exception&) {
          return ss::make_ready_future<batch_result>(batch_result{});
      })
      .handle_exception_type([](const ss::abort_requested_exception&) {
          return ss::make_ready_future<batch_result>(batch_result{});
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
    for (const auto& [key, meta] : manifest()) {
        retry_chain_node fib(
          _metadata_sync_timeout, _upload_loop_initial_backoff, &rtc);
        auto sname = cloud_storage::generate_segment_name(
          key.base_offset, key.term);
        auto spath = cloud_storage::generate_remote_segment_path(
          _ntp, _rev, sname, meta.archiver_term);
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

} // namespace archival
