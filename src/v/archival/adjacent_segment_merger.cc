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
      config::shard_local_cfg().cloud_storage_segment_size_min.bind()) {}

ss::future<> adjacent_segment_merger::stop() { return _gate.close(); }

ss::future<> adjacent_segment_merger::run(retry_chain_node& rtc) {
    ss::gate::holder h(_gate);
    vlog(_ctxlog.debug, "Adjacent segment merger run begin");
    auto scanner = [this](
                     model::offset local_start_offset,
                     const cloud_storage::partition_manifest& manifest)
      -> std::optional<adjacent_segment_run> {
        adjacent_segment_run run(_archiver.get_ntp());
        auto [low_watermark, high_watermark] = get_low_high_segment_size(
          _archiver.get_local_segment_size(),
          _min_segment_size,
          _target_segment_size);
        model::offset so = _last;
        if (so == model::offset{} && _is_local) {
            // Local lookup, start from local start offset
            so = local_start_offset;
        } else {
            // Remote lookup, start from start offset in the manifest (or 0)
            so = _archiver.manifest().get_start_offset().value_or(
              model::offset{0});
        }
        vlog(
          _ctxlog.debug,
          "Searching for adjacent segment run, start: {}, is_local: {}, "
          "local_start_offset: {}",
          so,
          _is_local,
          local_start_offset);
        for (auto it = manifest.segment_containing(so); it != manifest.end();
             it++) {
            if (
              !_is_local && it->second.committed_offset >= local_start_offset) {
                // We're looking for the remote segment
                break;
            }
            auto [key, meta] = *it;
            if (!run.maybe_add_segment(meta, high_watermark)) {
                continue;
            }
        }
        if (run.num_segments > 1 && run.meta.size_bytes > low_watermark) {
            vlog(_ctxlog.debug, "Found adjacent segment run {}", run);
            return run;
        }
        if (
          run.num_segments > 1
          && run.meta.committed_offset != manifest.get_last_offset()) {
            // Reupload if we have a run of small segments between large
            // segments but this run is smaller than low_watermark. In this case
            // its stil makes sense to reupload it.
            vlog(
              _ctxlog.debug,
              "Found adjacent segment run {} which is smaller than the limit "
              "{}",
              run,
              low_watermark);
            return run;
        }
        vlog(_ctxlog.debug, "Adjacent segment run not found");
        return std::nullopt;
    };
    auto upl = co_await _archiver.find_reupload_candidate(scanner);
    if (!upl.has_value()) {
        vlog(_ctxlog.debug, "No upload candidates");
        co_return;
    }
    auto next = model::next_offset(upl->candidate.final_offset);
    vlog(
      _ctxlog.debug, "Going to upload segment {}", upl->candidate.exposed_name);
    auto uploaded = co_await _archiver.upload(std::move(*upl), std::ref(rtc));
    if (uploaded) {
        _last = next;
    }
}

void adjacent_segment_merger::interrupt() { _as.request_abort(); }

bool adjacent_segment_merger::interrupted() const {
    return _as.abort_requested();
}

} // namespace archival