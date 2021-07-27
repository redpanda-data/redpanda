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

#include "archival/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
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
      "manifest_upload_timeout: {}}}",
      cfg.bucket_name,
      cfg.interval.count(),
      cfg.client_config,
      cfg.connection_limit,
      cfg.initial_backoff.count(),
      cfg.segment_upload_timeout.count(),
      cfg.manifest_upload_timeout.count());
    return o;
}

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp,
  const configuration& conf,
  cloud_storage::remote& remote,
  service_probe& svc_probe)
  : _svc_probe(svc_probe)
  , _probe(conf.ntp_metrics_disabled, ntp.ntp())
  , _ntp(ntp.ntp())
  , _rev(ntp.get_revision())
  , _remote(remote)
  , _policy(_ntp, _svc_probe, std::ref(_probe))
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

ss::future<cloud_storage::upload_result> ntp_archiver::upload_segment(
  upload_candidate candidate, retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(_segment_upload_timeout, _initial_backoff, &parent);
    retry_chain_logger ctxlog(archival_log, fib, _ntp.path());
    vlog(
      ctxlog.debug,
      "Uploading segment for {}, exposed name {} offset {}, length {}",
      _ntp,
      candidate.exposed_name,
      candidate.starting_offset,
      candidate.content_length);

    auto reset_func = [candidate] {
        auto stream = candidate.source->reader().data_stream(
          candidate.file_offset, ss::default_priority_class());
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

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  storage::log_manager& lm,
  model::offset high_watermark,
  retry_chain_node& parent) {
    retry_chain_logger ctxlog(archival_log, parent, _ntp.path());
    gate_guard guard{_gate};
    auto mlock = co_await ss::get_units(_mutex, 1);
    vlog(ctxlog.debug, "Uploading next candidates called for {}", _ntp);
    ntp_archiver::batch_result total{};
    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains dirty_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if there is no segments.
    auto last_uploaded_offset = _manifest.size() ? _manifest.get_last_offset()
                                                     + model::offset(1)
                                                 : model::offset(0);
    std::vector<ss::future<cloud_storage::upload_result>> flist;
    std::vector<cloud_storage::manifest::segment_meta> meta;
    std::vector<ss::sstring> names;
    std::vector<model::offset> deltas;
    for (size_t i = 0; i < _concurrency; i++) {
        vlog(
          ctxlog.debug,
          "Uploading next candidates for {}, trying offset {}",
          _ntp,
          last_uploaded_offset);
        auto upload = _policy.get_next_candidate(
          last_uploaded_offset, high_watermark, lm);
        if (upload.source.get() == nullptr) {
            vlog(
              ctxlog.debug, "Uploading next candidates for {}, ...skip", _ntp);
            break;
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
                last_uploaded_offset = meta->committed_offset
                                       + model::offset(1);
                continue;
            }
        }
        auto offset = upload.source->offsets().dirty_offset;
        auto base = upload.source->offsets().base_offset;
        last_uploaded_offset = offset + model::offset(1);
        deltas.push_back(offset - base);
        flist.emplace_back(upload_segment(upload, parent));
        cloud_storage::manifest::segment_meta m{
          .is_compacted = upload.source->is_compacted_segment(),
          .size_bytes = upload.content_length,
          .base_offset = upload.starting_offset,
          .committed_offset = offset,
        };
        meta.emplace_back(m);
        names.emplace_back(upload.exposed_name);
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
        _probe.uploaded(deltas[i]);
        _manifest.add(segment_name(names[i]), meta[i]);
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

} // namespace archival
