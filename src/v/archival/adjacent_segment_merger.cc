/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/adjacent_segment_merger.h"

#include "archival/ntp_archiver_service.h"
#include "archival/segment_reupload.h"
#include "archival/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "storage/disk_log_impl.h"
#include "utils/human.h"

namespace archival {

static std::pair<size_t, size_t> get_low_high_segment_size(
  size_t segment_size,
  config::binding<std::optional<size_t>>& low_wm,
  config::binding<std::optional<size_t>>& high_wm) {
    auto high_watermark = high_wm().value_or(segment_size);
    auto low_watermark = low_wm().value_or(high_watermark / 2);
    if (low_watermark >= high_watermark) {
        // Low watermark can't be equal to high watermark
        // otherwise the merger want be able to find upload
        // candidate.
        low_watermark = high_watermark * 8 / 10;
    }
    return std::make_pair(low_watermark, high_watermark);
}

adjacent_segment_merger::adjacent_segment_merger(
  ntp_archiver& parent, retry_chain_logger& ctxlog, bool is_local)
  : _is_local(is_local)
  , _archiver(parent)
  , _ctxlog(ctxlog)
  , _target_segment_size(
      config::shard_local_cfg().cloud_storage_segment_size_target.bind())
  , _min_segment_size(
      config::shard_local_cfg().cloud_storage_segment_size_min.bind()) {
    vassert(
      !_archiver.ntp_config().is_read_replica_mode_enabled(),
      "Constructed adjacent segment merger on read replica {}",
      _archiver.get_ntp());
}

ss::future<> adjacent_segment_merger::stop() { return _gate.close(); }

void adjacent_segment_merger::set_enabled(bool enabled) { _enabled = enabled; }

void adjacent_segment_merger::acquire() { _holder = ss::gate::holder(_gate); }

void adjacent_segment_merger::release() { _holder.release(); }

std::optional<adjacent_segment_run> adjacent_segment_merger::scan_manifest(
  model::offset local_start_offset,
  const cloud_storage::partition_manifest& manifest) {
    auto [min_segment_size, max_segment_size] = get_low_high_segment_size(
      _archiver.get_local_segment_size(),
      _min_segment_size,
      _target_segment_size);
    model::offset so = _last;
    if (so == model::offset{} && _is_local) {
        // Local lookup, start from local start offset
        so = local_start_offset;
    } else {
        // Remote lookup, start from start offset in the manifest (or 0)
        so = _archiver.manifest().get_start_offset().value_or(model::offset{0});
    }
    vlog(
      _ctxlog.debug,
      "Searching for adjacent segment run, start: {}, is_local: {}, "
      "local_start_offset: {}, low watermark: {}, high watermark: {}",
      so,
      _is_local,
      local_start_offset,
      min_segment_size,
      max_segment_size);

    adjacent_segment_run run(_archiver.get_ntp());
    for (auto it = manifest.segment_containing(so); it != manifest.end();
         it++) {
        if (!_is_local && it->second.committed_offset >= local_start_offset) {
            // We're looking for the remote segment
            break;
        }
        auto [key, meta] = *it;
        if (run.maybe_add_segment(meta, max_segment_size)) {
            // We have found a run whith the size close to max_segment_size
            // and can proceed early.
            break;
        }
    }
    if (run.num_segments > 1 && run.meta.size_bytes > min_segment_size) {
        // Normal reupload, the upload candidate is larger than
        // min_segment_size and contains more than one segments.
        vlog(_ctxlog.debug, "Found adjacent segment run {}", run);
        return run;
    }
    if (
      run.num_segments > 1
      && run.meta.committed_offset != manifest.get_last_offset()) {
        // Reupload if we have a run of small segments between large
        // segments but this run is smaller than min_segment_size. In this
        // case its stil makes sense to reupload it.
        vlog(
          _ctxlog.debug,
          "Found adjacent segment run {} which is smaller than the "
          "limit "
          "{}",
          run,
          min_segment_size);
        return run;
    }
    vlog(
      _ctxlog.debug,
      "Adjacent segment run not found, num {}, segments {}, size-bytes "
      "{}, "
      "offset range {} - {}",
      run.num_segments,
      run.segments.size(),
      run.meta.size_bytes,
      run.meta.base_offset,
      run.meta.committed_offset);
    return std::nullopt;
}

ss::future<housekeeping_job::run_result>
adjacent_segment_merger::run(retry_chain_node& rtc, run_quota_t quota) {
    ss::gate::holder h(_gate);
    run_result result{
      .status = run_status::skipped,
      .consumed = run_quota_t(0),
      .remaining = quota,
    };
    vlog(
      _ctxlog.debug,
      "Adjacent segment merger run begin, last offset is {}",
      _last);
    for (int i = 0; i < max_reuploads_per_run; i++) {
        if (
          !_enabled || _as.abort_requested()
          || _archiver.manifest().get_last_offset() == model::offset::max()) {
            // Avoid reuploading anything if last offset is max. This can only
            // happen if the recovery was incomplete and by reuploading any data
            // we can corrupt metadata in the cloud.
            co_return result;
        }
        if (result.remaining <= 0) {
            co_return result;
        }
        auto scanner = [this](
                         model::offset local_start_offset,
                         const cloud_storage::partition_manifest& manifest) {
            return scan_manifest(local_start_offset, manifest);
        };
        auto upl = co_await _archiver.find_reupload_candidate(scanner);
        if (!upl.has_value()) {
            vlog(_ctxlog.debug, "No more upload candidates");
            co_return result;
        }
        auto next = model::next_offset(upl->candidate.final_offset);
        vlog(
          _ctxlog.debug,
          "Going to upload segment {}, num source segments {}, last offset {}",
          upl->candidate.exposed_name,
          upl->candidate.sources.size(),
          upl->candidate.final_offset);
        for (const auto& src : upl->candidate.sources) {
            vlog(
              _ctxlog.debug,
              "Local log segment {} found, size {}",
              src->filename(),
              src->size_bytes());
        }
        auto uploaded = co_await _archiver.upload(
          std::move(*upl), std::ref(rtc));
        if (uploaded) {
            _last = next;
            result.status = run_status::ok;
            result.local_reuploads += 1;
            result.manifest_uploads += 1;
            result.metadata_syncs += 1;
            result.consumed = result.consumed + run_quota_t{1};
            result.remaining = result.remaining - run_quota_t{1};
            vlog(
              _ctxlog.debug,
              "Successfuly uploaded segment, new last offfset is {}",
              _last);
        } else {
            // Upload failed
            result.status = run_status::failed;
            vlog(
              _ctxlog.debug,
              "Failed to upload segment, last offfset is {}",
              _last);
        }
    }
    vlog(
      _ctxlog.debug,
      "Adjacent segment merger run completed, last offset is {}",
      _last);
    co_return result;
}

void adjacent_segment_merger::interrupt() { _as.request_abort(); }

bool adjacent_segment_merger::interrupted() const {
    return _as.abort_requested();
}

} // namespace archival
