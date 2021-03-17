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
      "{}}}",
      cfg.bucket_name,
      cfg.interval.count(),
      cfg.client_config,
      cfg.connection_limit);
    return o;
}

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp, const configuration& conf)
  : _ntp(ntp.ntp())
  , _rev(ntp.get_revision())
  , _client_conf(conf.client_config)
  , _policy(_ntp)
  , _bucket(conf.bucket_name)
  , _remote(_ntp, _rev)
  , _gate() {
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

ss::future<download_manifest_result> ntp_archiver::download_manifest() {
    gate_guard guard{_gate};
    auto key = _remote.get_manifest_path();
    vlog(archival_log.debug, "Download manifest {}", key());
    auto path = s3::object_key(key().string());
    s3::client client(_client_conf, _as);
    auto result = download_manifest_result::success;
    try {
        auto resp = co_await client.get_object(_bucket, path);
        vlog(archival_log.debug, "Receive OK response from {}", path);
        co_await _remote.update(resp->as_input_stream());
    } catch (const s3::rest_error_response& err) {
        if (err.code() == s3::s3_error_code::no_such_key) {
            // This can happen when we're dealing with new partition for which
            // manifest wasn't uploaded. But also, this can appen if we uploaded
            // the first segment and crashed before we were able to upload the
            // manifest. This shouldn't be the problem though. We will just
            // re-upload this segment for once.
            vlog(archival_log.debug, "NoSuchKey response received {}", path);
            result = download_manifest_result::notfound;
        } else if (err.code() == s3::s3_error_code::slow_down) {
            // This can happen when we're dealing with high request rate to the
            // manifest's prefix.
            vlog(archival_log.debug, "SlowDown response received {}", path);
            result = download_manifest_result::backoff;
        } else {
            throw;
        }
    }
    co_await client.shutdown();
    co_return result;
}

ss::future<> ntp_archiver::upload_manifest() {
    gate_guard guard{_gate};
    vlog(archival_log.debug, "Uploading manifest for {}", _ntp);
    ss::lowres_clock::duration backoff = 4ms;
    int backoff_quota = 8; // max backoff time should be close to 10s
    auto key = _remote.get_manifest_path();
    vlog(archival_log.trace, "Upload manifest {}", key());
    auto path = s3::object_key(key().string());
    std::vector<s3::object_tag> tags = {{"rp-type", "partition-manifest"}};
    while (!_gate.is_closed() && backoff_quota-- > 0) {
        bool slowdown = false;
        s3::client client(_client_conf, _as);
        try {
            auto [is, size] = _remote.serialize();
            co_await client.put_object(
              _bucket, path, size, std::move(is), tags);
            co_await client.shutdown();
        } catch (const s3::rest_error_response& err) {
            vlog(
              archival_log.error,
              "Uploading manifest for {}, {} error detected, code: {}, "
              "request_id: {}, resource: {}",
              _ntp,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            if (err.code() == s3::s3_error_code::slow_down) {
                slowdown = true;
            } else {
                throw;
            }
        } catch (...) {
            vlog(
              archival_log.error,
              "Uploading manifest for {}, unexpected error: {}",
              _ntp,
              std::current_exception());
        }
        if (slowdown) {
            // Apply exponential backoff because S3 asked us
            vlog(
              archival_log.debug,
              "Uploading manifest for {}, {}ms backoff required",
              _ntp,
              backoff.count());
            co_await ss::sleep_abortable(
              backoff + _backoff.next_jitter_duration(), _as);
            backoff *= 2;
            continue;
        }
        break;
    }
    if (backoff_quota == 0) {
        // We exceded backoff quota, warn user and continue. The manifest
        // should be re-uploaded with the next uploaded segment.
        vlog(
          archival_log.warn,
          "Uploading manifest for {}, backoff quota exceded, manifest {} not "
          "uploaded",
          _ntp,
          path);
    }
    co_return;
}

const manifest& ntp_archiver::get_remote_manifest() const { return _remote; }

ss::future<bool> ntp_archiver::upload_segment(
  ss::semaphore& req_limit, upload_candidate candidate) {
    gate_guard guard{_gate};
    vlog(
      archival_log.debug,
      "Uploading segment for {}, exposed name {} offset {}, length {}",
      _ntp,
      candidate.exposed_name,
      candidate.starting_offset,
      candidate.content_length);
    ss::lowres_clock::duration backoff = 4ms;
    int backoff_quota = 8; // max backoff time should be close to 10s
    auto s3path = _remote.get_remote_segment_path(
      segment_name(candidate.exposed_name));
    std::vector<s3::object_tag> tags = {{"rp-type", "segment"}};
    while (!_gate.is_closed() && backoff_quota-- > 0) {
        auto units = co_await ss::get_units(req_limit, 1);
        s3::client client(_client_conf, _as);
        auto stream = candidate.source->reader().data_stream(
          candidate.file_offset, ss::default_priority_class());
        bool slowdown = false;
        vlog(
          archival_log.debug,
          "Uploading segment for {}, path {}",
          _ntp,
          s3path);
        try {
            // Segment upload attempt
            co_await client.put_object(
              _bucket,
              s3::object_key(s3path().string()),
              candidate.content_length,
              std::move(stream),
              tags);
            co_await client.shutdown();
        } catch (const s3::rest_error_response& err) {
            vlog(
              archival_log.error,
              "Uploading segment for {}, path {}, {} error detected, code: {}, "
              "request_id: {}, resource: {}",
              _ntp,
              s3path,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            if (err.code() == s3::s3_error_code::slow_down) {
                slowdown = true;
            } else {
                co_return false;
            }
        } catch (...) {
            vlog(
              archival_log.error,
              "Failed to upload segment for {}, path {}. Reason: {}",
              _ntp,
              s3path,
              std::current_exception());
            co_return false;
        }
        if (slowdown) {
            // Apply exponential backoff because S3 asked us
            vlog(
              archival_log.debug,
              "Uploading segment for {}, {}ms backoff required",
              _ntp,
              backoff.count());
            co_await ss::sleep_abortable(
              backoff + _backoff.next_jitter_duration(), _as);
            backoff *= 2;
            continue;
        }
        break;
    }
    co_return true;
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  ss::semaphore& req_limit, storage::log_manager& lm) {
    vlog(archival_log.debug, "Uploading next candidates called for {}", _ntp);
    gate_guard guard{_gate};
    auto mlock = co_await ss::get_units(_mutex, 1);
    ntp_archiver::batch_result total{};
    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains committed_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if there is no segments.
    auto offset = _remote.size() ? _remote.get_last_offset() + model::offset(1)
                                 : model::offset(0);
    std::vector<ss::future<bool>> flist;
    std::vector<manifest::segment_meta> meta;
    std::vector<ss::sstring> names;
    for (size_t i = 0; i < _concurrency; i++) {
        vlog(
          archival_log.debug,
          "Uploading next candidates for {}, trying offset {}",
          _ntp,
          offset);
        auto upload = _policy.get_next_candidate(offset, lm);
        if (upload.source.get() == nullptr) {
            vlog(
              archival_log.debug,
              "Uploading next candidates for {}, ...skip",
              _ntp);
            break;
        }
        if (_remote.contains(upload.exposed_name)) {
            // This sholdn't happen normally and indicates an error (e.g.
            // manifest doesn't match the actual data because it was uploaded by
            // different cluster or altered). We can just skip the segment.
            vlog(
              archival_log.warn,
              "Uploading next candidates for {}, attempt to re-upload {}",
              _ntp,
              upload);
            const auto& meta = _remote.get(upload.exposed_name);
            offset = meta->committed_offset + model::offset(1);
            continue;
        }
        offset = upload.source->offsets().committed_offset + model::offset(1);
        flist.emplace_back(upload_segment(req_limit, upload));
        manifest::segment_meta m{
          .is_compacted = upload.source->is_compacted_segment(),
          .size_bytes
          = upload.source->size_bytes()
            - (upload.starting_offset - upload.source->offsets().base_offset),
          .base_offset = upload.starting_offset,
          .committed_offset = upload.source->offsets().committed_offset,
        };
        meta.emplace_back(m);
        names.emplace_back(upload.exposed_name);
    }
    if (flist.empty()) {
        vlog(
          archival_log.debug,
          "Uploading next candidates for {}, no uploads started ...skip",
          _ntp);
        co_return total;
    }
    auto results = co_await ss::when_all_succeed(begin(flist), end(flist));
    total.num_succeded = std::count(begin(results), end(results), true);
    total.num_failed = std::count(begin(results), end(results), false);
    for (size_t i = 0; i < results.size(); i++) {
        if (!results[i]) {
            break;
        }
        _remote.add(segment_name(names[i]), meta[i]);
    }
    if (total.num_succeded != 0) {
        vlog(
          archival_log.debug,
          "Uploading next candidates for {}, re-uploading manifest file",
          _ntp);
        co_await upload_manifest();
        _last_upload_time = ss::lowres_clock::now();
    }
    co_return total;
}

} // namespace archival
