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

local_adjacent_segment_merger::local_adjacent_segment_merger(
  ntp_archiver& parent, retry_chain_logger& ctxlog)
  : _archiver(parent)
  , _ctxlog(ctxlog)
  , _target_segment_size(
      config::shard_local_cfg().cloud_storage_segment_size_target.bind())
  , _min_segment_size(
      config::shard_local_cfg().cloud_storage_segment_size_min.bind()) {}

ss::future<> local_adjacent_segment_merger::stop() {
    interrupt();
    return _gate.close();
}

ss::future<> local_adjacent_segment_merger::run(retry_chain_node& rtc) {
    ss::gate::holder h(_gate);
    vlog(_ctxlog.debug, "Adjacent segment merger run begin");
    auto scanner = [this](
                     model::offset start_offset,
                     const cloud_storage::partition_manifest& manifest)
      -> std::optional<adjacent_segment_run> {
        adjacent_segment_run run;
        auto high_watermark = _target_segment_size().value_or(_segment_size());
        auto low_watermark = _min_segment_size().value_or(high_watermark / 2);
        if (low_watermark >= high_watermark) {
            // Low watermark can't be equal to high watermark
            // otherwise the merger want be able to find upload
            // candidate.
            low_watermark = high_watermark * 8 / 10;
        }
        auto so = _last == model::offset{} ? start_offset : _last;
        for (auto it = manifest.segment_containing(so); it != manifest.end();
             it++) {
            auto [key, meta] = *it;
            if (!run.maybe_add_segment(meta, high_watermark)) {
                continue;
            }
        }
        if (run.num_segments > 1 && run.size_bytes > low_watermark) {
            return run;
        }
        return std::nullopt;
    };
    auto upl = co_await _archiver.find_upload_candidate(scanner);
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

void local_adjacent_segment_merger::interrupt() { _as.request_abort(); }

bool local_adjacent_segment_merger::interrupted() const {
    return _as.abort_requested();
}

} // namespace archival