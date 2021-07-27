/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"

#include "archival/archival_policy.h"
#include "archival/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "raft/configuration_manager.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/parser.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <exception>
#include <stdexcept>

namespace archival {

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{bucket_name: {}, interval: {}, client_config: {}, connection_limit: "
      "{}, initial_backoff: {}, segment_upload_timeout: {}, "
      "manifest_upload_timeout: {}, time_limit: {}}}",
      cfg.bucket_name,
      cfg.interval.count(),
      cfg.client_config,
      cfg.connection_limit,
      cfg.initial_backoff.count(),
      cfg.segment_upload_timeout.count(),
      cfg.manifest_upload_timeout.count(),
      cfg.time_limit);
    return o;
}

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp,
  const configuration& conf,
  cloud_storage::remote& remote,
  ss::lw_shared_ptr<cluster::partition> part,
  service_probe& svc_probe)
  : _svc_probe(svc_probe)
  , _probe(conf.ntp_metrics_disabled, ntp.ntp())
  , _ntp(ntp.ntp())
  , _rev(ntp.get_revision())
  , _remote(remote)
  , _partition(std::move(part))
  , _policy(_ntp, _svc_probe, std::ref(_probe), conf.time_limit)
  , _bucket(conf.bucket_name)
  , _manifest(_ntp, _rev)
  , _gate()
  , _initial_backoff(conf.initial_backoff)
  , _segment_upload_timeout(conf.segment_upload_timeout)
  , _manifest_upload_timeout(conf.manifest_upload_timeout) {
    vlog(archival_log.trace, "Create ntp_archiver {}", _ntp.path());
}

ss::future<> ntp_archiver::stop() {
    _as.request_abort();
    return _gate.close();
}

const model::ntp& ntp_archiver::get_ntp() const { return _ntp; }

model::revision_id ntp_archiver::get_revision_id() const { return _rev; }

const ss::lowres_clock::time_point ntp_archiver::get_last_upload_time() const {
    return _last_upload_time;
}

const cloud_storage::manifest& ntp_archiver::get_remote_manifest() const {
    return _manifest;
}

ss::future<cloud_storage::download_result>
ntp_archiver::download_manifest(retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(_manifest_upload_timeout, _initial_backoff, &parent);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    vlog(ctxlog.debug, "Downloading manifest for {}", _ntp);
    auto path = _manifest.get_manifest_path();
    auto key = cloud_storage::remote_manifest_path(
      std::filesystem::path(std::move(path)));
    co_return co_await _remote.download_manifest(_bucket, key, _manifest, fib);
}

ss::future<cloud_storage::upload_result>
ntp_archiver::upload_manifest(retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(_manifest_upload_timeout, _initial_backoff, &parent);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    vlog(ctxlog.debug, "Uploading manifest for {}", _ntp);
    co_return co_await _remote.upload_manifest(_bucket, _manifest, fib);
}

// from offset to offset (by record batch boundary)
ss::future<cloud_storage::upload_result> ntp_archiver::upload_segment(
  upload_candidate candidate, retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(_segment_upload_timeout, _initial_backoff, &parent);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    vlog(ctxlog.debug, "Uploading segment {}", candidate);

    auto reset_func = [candidate] {
        auto stream = candidate.source->reader().data_stream(
          candidate.file_offset,
          candidate.final_file_offset,
          ss::default_priority_class());
        return stream;
    };
    co_return co_await _remote.upload_segment(
      _bucket,
      candidate.exposed_name,
      candidate.content_length,
      reset_func,
      _manifest,
      fib);
}

static model::offset num_config_batches_before_offset(
  const raft::configuration_manager& cm, model::offset o) {
    auto it = cm.lower_bound(o);
    if (it != cm.begin()) {
        --it;
    }
    return model::offset(it->second.idx());
}

ss::future<ntp_archiver::scheduled_upload> ntp_archiver::schedule_single_upload(
  storage::log_manager& lm,
  model::offset last_uploaded_offset,
  model::offset last_stable_offset,
  retry_chain_node& parent) {
    retry_chain_logger ctxlog(archival_log, parent, _ntp.path());

    auto upload = co_await _policy.get_next_candidate(
      last_uploaded_offset, last_stable_offset, lm);

    if (upload.source.get() == nullptr) {
        vlog(
          ctxlog.debug,
          "Uploading next candidates, last_uploaded_offset: {}, "
          "last_stable_offset: {} ...skip",
          last_uploaded_offset,
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
    if (_manifest.contains(upload.exposed_name)) {
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
        const auto& meta = _manifest.get(upload.exposed_name);
        auto dirty_offset = upload.source->offsets().dirty_offset;
        if (meta->committed_offset < dirty_offset) {
            vlog(
              ctxlog.info,
              "Uploading next candidates for {}, attempt to re-upload "
              "{}, manifest committed offset {}, upload dirty offset {}",
              _ntp,
              upload,
              meta->committed_offset,
              dirty_offset);
        } else if (meta->committed_offset >= dirty_offset) {
            vlog(
              ctxlog.warn,
              "Skip upload for {} because it's already in the manifest "
              "{}, manifest committed offset {}, upload dirty offset {}",
              _ntp,
              upload,
              meta->committed_offset,
              dirty_offset);
            last_uploaded_offset = meta->committed_offset;
            co_return scheduled_upload{
              .result = std::nullopt,
              .inclusive_last_offset = last_uploaded_offset,
              .meta = std::nullopt,
              .name = std::nullopt,
              .delta = std::nullopt,
              .stop = ss::stop_iteration::no,
            };
        }
    }
    auto offset = upload.final_offset;
    auto base = upload.starting_offset;
    last_uploaded_offset = offset + model::offset(1);
    co_return scheduled_upload{
      .result = upload_segment(upload, parent),
      .inclusive_last_offset = offset,
      .meta = cloud_storage::manifest::segment_meta{
        .is_compacted = upload.source->is_compacted_segment(),
        .size_bytes = upload.content_length,
        .base_offset = upload.starting_offset,
        .committed_offset = offset,
        .base_timestamp = upload.base_timestamp,
        .max_timestamp = upload.max_timestamp,
        .delta_offset = num_config_batches_before_offset(_partition->get_cfg_manager(), base),
      }, 
      .name = upload.exposed_name, .delta = offset - base,
      .stop = ss::stop_iteration::no,
    };
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(
  storage::log_manager& lm,
  model::offset last_stable_offset,
  retry_chain_node& parent) {
    retry_chain_logger ctxlog(archival_log, parent, _ntp.path());

    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains dirty_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if there is no segments.
    auto last_uploaded_offset = _manifest.size() ? _manifest.get_last_offset()
                                                     + model::offset(1)
                                                 : model::offset(0);

    vlog(ctxlog.debug, "Uploading next candidates called for {}", _ntp);

    vlog(
      ctxlog.debug,
      "Uploading next candidates for {}, trying offset {}, LSO {}",
      _ntp,
      last_uploaded_offset,
      last_stable_offset);

    return ss::do_with(
      std::vector<scheduled_upload>(),
      size_t(0),
      last_uploaded_offset,
      [this, &lm, last_stable_offset, &parent](
        std::vector<scheduled_upload>& scheduled,
        size_t& ix,
        model::offset& last_uploaded_offset) {
          return ss::repeat([this,
                             &lm,
                             &last_uploaded_offset,
                             last_stable_offset,
                             &parent,
                             &scheduled,
                             &ix] {
                     if (ix == _concurrency) {
                         return ss::make_ready_future<ss::stop_iteration>(
                           ss::stop_iteration::yes);
                     }
                     ++ix;
                     return schedule_single_upload(
                              lm,
                              last_uploaded_offset,
                              last_stable_offset,
                              parent)
                       .then([&scheduled,
                              &last_uploaded_offset](scheduled_upload upload) {
                           auto res = upload.stop;
                           last_uploaded_offset = upload.inclusive_last_offset
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
  std::vector<ntp_archiver::scheduled_upload> scheduled,
  retry_chain_node& parent) {
    retry_chain_logger ctxlog(archival_log, parent, _ntp.path());
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
        vlog(
          ctxlog.debug,
          "Uploading next candidates for {}, no uploads started ...skip",
          _ntp);
        co_return total;
    }
    auto results = co_await ss::when_all_succeed(begin(flist), end(flist));
    total.num_succeded = std::count(
      begin(results), end(results), cloud_storage::upload_result::success);
    total.num_failed = flist.size() - total.num_succeded;
    for (size_t i = 0; i < results.size(); i++) {
        if (results[i] != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];
        _probe.uploaded(*upload.delta);
        _manifest.add(segment_name(*upload.name), *upload.meta);
    }
    if (total.num_succeded != 0) {
        vlog(
          ctxlog.debug,
          "Uploading next candidates for {}, re-uploading manifest file",
          _ntp);
        auto res = co_await upload_manifest(parent);
        if (res != cloud_storage::upload_result::success) {
            vlog(ctxlog.debug, "Manifest upload for {} failed", _ntp);
        }
        _last_upload_time = ss::lowres_clock::now();
    }
    co_return total;
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  storage::log_manager& lm,
  retry_chain_node& parent,
  std::optional<model::offset> lso_override) {
    retry_chain_logger ctxlog(archival_log, parent, _ntp.path());
    vlog(ctxlog.debug, "Uploading next candidates called for {}", _ntp);
    auto last_stable_offset = lso_override ? *lso_override
                                           : _partition->last_stable_offset();
    return ss::with_gate(_gate, [this, &lm, last_stable_offset, &parent] {
        return ss::with_semaphore(
          _mutex, 1, [this, &lm, last_stable_offset, &parent] {
              return schedule_uploads(lm, last_stable_offset, parent)
                .then([this, &parent](std::vector<scheduled_upload> scheduled) {
                    return wait_all_scheduled_uploads(
                      std::move(scheduled), parent);
                });
          });
    });
}

} // namespace archival
