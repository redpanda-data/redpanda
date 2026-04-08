/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "segment_reupload.h"

#include "base/vlog.h"
#include "cloud_storage/partition_manifest.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/types.h"
#include "config/configuration.h"
#include "logger.h"
#include "model/fundamental.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/offset_to_filepos.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>

#include <ranges>
#include <utility>

namespace archival {

bool eligible_for_compacted_reupload(const storage::segment& s) {
    if (config::shard_local_cfg().log_compaction_use_sliding_window) {
        return s.finished_windowed_compaction();
    }
    return s.has_self_compact_timestamp();
}

std::ostream& operator<<(std::ostream& s, const upload_candidate& c) {
    vassert(
      c.sources.empty() || c.remote_sources.empty(),
      "The upload candidate could have only local or only remote source");
    if (c.sources.empty() && c.remote_sources.empty()) {
        s << "{empty}";
        return s;
    }

    std::vector<ss::sstring> source_names;
    source_names.reserve(std::max(c.sources.size(), c.remote_sources.size()));
    if (c.remote_sources.empty()) {
        std::transform(
          c.sources.begin(),
          c.sources.end(),
          std::back_inserter(source_names),
          [](const auto& src) { return src->filename(); });
    } else if (c.sources.empty()) {
        std::transform(
          c.remote_sources.begin(),
          c.remote_sources.end(),
          std::back_inserter(source_names),
          [](const auto& src) { return src().native(); });
    }

    fmt::print(
      s,
      "{{source segment offsets: {}, exposed_name: {}, starting_offset: {}, "
      "file_offset: {}, content_length: {}, final_offset: {}, "
      "final_file_offset: {}, term: {}, source names: {}}}",
      c.sources.front()->offsets(),
      c.exposed_name,
      c.starting_offset,
      c.file_offset,
      c.content_length,
      c.final_offset,
      c.final_file_offset,
      c.term,
      source_names);
    return s;
}

std::ostream& operator<<(std::ostream& s, const segment_collector_stream& c) {
    if (c.size == 0) {
        return s << "{empty}";
    }
    fmt::print(
      s,
      "{{starting_offset: {}, content_length: {}, final_offset: {}, term: {}}}",
      c.start_offset,
      c.size,
      c.end_offset,
      c.term);
    return s;
}

std::ostream& operator<<(std::ostream& os, candidate_creation_error err) {
    os << "candidate creation error: ";
    switch (err) {
    case candidate_creation_error::no_segments_collected:
        return os << "no segments collected";
    case candidate_creation_error::begin_offset_seek_error:
        return os << "failed to seek begin offset";
    case candidate_creation_error::end_offset_seek_error:
        return os << "failed to seek end offset";
    case candidate_creation_error::offset_inside_batch:
        return os << "offset inside batch";
    case candidate_creation_error::upload_size_unchanged:
        return os << "size of candidate unchanged";
    case candidate_creation_error::cannot_replace_manifest_entry:
        return os << "candidate cannot replace manifest entry";
    case candidate_creation_error::no_segment_for_begin_offset:
        return os << "no segment for begin offset";
    case candidate_creation_error::missing_ntp_config:
        return os << "missing config for NTP";
    case candidate_creation_error::failed_to_get_file_range:
        return os << "failed to get file range for candidate";
    case candidate_creation_error::zero_content_length:
        return os << "candidate has no content";
    case candidate_creation_error::concurrency_error:
        return os << "collected segments are modified concurrently";
    }
}

ss::log_level log_level_for_error(const candidate_creation_error& error) {
    switch (error) {
    case candidate_creation_error::no_segments_collected:
    case candidate_creation_error::begin_offset_seek_error:
    case candidate_creation_error::end_offset_seek_error:
    case candidate_creation_error::upload_size_unchanged:
    case candidate_creation_error::cannot_replace_manifest_entry:
    case candidate_creation_error::no_segment_for_begin_offset:
    case candidate_creation_error::failed_to_get_file_range:
    case candidate_creation_error::zero_content_length:
    case candidate_creation_error::concurrency_error:
        return ss::log_level::debug;
    case candidate_creation_error::offset_inside_batch:
    case candidate_creation_error::missing_ntp_config:
        return ss::log_level::warn;
    }
}

std::ostream&
operator<<(std::ostream& os, const skip_offset_range& skip_range) {
    fmt::print(
      os,
      "skip_offset_range{{begin: {}, end: {}, error: {}}}",
      skip_range.begin_offset,
      skip_range.end_offset,
      skip_range.reason);
    return os;
}

segment_collector::segment_collector(
  segment_collector_mode mode,
  model::offset begin_inclusive,
  const cloud_storage::partition_manifest& manifest,
  const storage::log& log,
  size_t max_uploaded_segment_size,
  std::optional<model::offset> end_inclusive,
  std::optional<model::offset> end_exclusive,
  std::optional<model::offset> flush_offset)
  : _begin_inclusive(begin_inclusive)
  , _manifest(manifest)
  , _log(log)
  , _max_uploaded_segment_size(max_uploaded_segment_size)
  , _target_end_inclusive(end_inclusive)
  , _collected_size(0)
  , _end_exclusive(end_exclusive)
  , _flush_offset(flush_offset)
  , _mode(mode) {}

segment_collector::segment_collector(
  segment_collector_mode mode,
  model::offset begin_inclusive,
  const cloud_storage::partition_manifest& manifest,
  const storage::log& log,
  size_t max_uploaded_segment_size,
  std::optional<model::offset> end_inclusive)
  : segment_collector(
      mode,
      begin_inclusive,
      manifest,
      log,
      max_uploaded_segment_size,
      end_inclusive,
      std::nullopt /* end_exclusive */,
      std::nullopt /* flush_offset */) {}

namespace {
static bool is_reupload_mode(segment_collector_mode mode) {
    return mode == segment_collector_mode::compacted_reupload
           || mode == segment_collector_mode::non_compacted_reupload;
}
} // namespace

void segment_collector::collect_segments() {
    if (_manifest.size() == 0 && is_reupload_mode(_mode)) {
        vlog(
          archival_log.debug,
          "No segments to collect for ntp {}, manifest empty",
          _manifest.get_ntp());
        return;
    }

    // start_offset < log start due to eviction of segments before
    // they could be uploaded, skip forward to log start.
    if (_begin_inclusive < _log.offsets().start_offset) {
        switch (_mode) {
        case segment_collector_mode::new_upload:
        case segment_collector_mode::compacted_reupload:
            vlog(
              archival_log.debug,
              "Provided start offset is below the start offset of the local "
              "log: "
              "{} < {} for ntp {}. Advancing to the beginning of the local "
              "log.",
              _begin_inclusive,
              _log.offsets().start_offset,
              _manifest.get_ntp());
            _begin_inclusive = _log.offsets().start_offset;
            break;
        case segment_collector_mode::non_compacted_reupload:
            vlog(
              archival_log.debug,
              "Provided start offset is below the start offset of the local "
              "log: "
              "{} < {} for ntp {}. Exiting early.",
              _begin_inclusive,
              _log.offsets().start_offset,
              _manifest.get_ntp());
            return;
        }
    }

    // Handle begin offset alignment to manifest segment boundary (if
    // required).
    switch (_mode) {
    case segment_collector_mode::compacted_reupload:
        align_begin_offset_to_manifest();
        break;
    case segment_collector_mode::non_compacted_reupload:
        if (_manifest.find(_begin_inclusive) == _manifest.end()) {
            vlog(
              archival_log.debug,
              "Provided start offset {} is not aligned to a segment in the "
              "manifest: for ntp {}. Exiting early.",
              _begin_inclusive,
              _manifest.get_ntp());
            return;
        }
        break;
    case segment_collector_mode::new_upload:
        if (_manifest.get_last_offset() > _begin_inclusive) {
            vlog(
              archival_log.debug,
              "Provided start offset {} is behind the manifest last offset "
              "{}: for ntp {}. Exiting early.",
              _begin_inclusive,
              _manifest.get_last_offset(),
              _manifest.get_ntp());
            return;
        }
        break;
    }

    if (
      is_reupload_mode(_mode)
      && _begin_inclusive > _manifest.get_last_offset()) {
        vlog(
          archival_log.warn,
          "Start offset {} is ahead of manifest last offset {} for ntp {}, not "
          "a reupload",
          _begin_inclusive,
          _manifest.get_last_offset(),
          _manifest.get_ntp());
        return;
    }

    if (_target_end_inclusive.has_value()) {
        if (_target_end_inclusive.value() < _log.offsets().start_offset) {
            vlog(
              archival_log.debug,
              "Provided end offset is below the start offset of the local log: "
              "{} < {} for ntp {}. Advancing to the beginning of the local "
              "log.",
              _target_end_inclusive.value(),
              _log.offsets().start_offset,
              _manifest.get_ntp());
            return;
        }
        if (
          is_reupload_mode(_mode)
          && _target_end_inclusive.value() > _manifest.get_last_offset()) {
            vlog(
              archival_log.debug,
              "Target end offset {} is ahead of manifest last offset {} for "
              "ntp {}",
              _target_end_inclusive.value(),
              _manifest.get_last_offset(),
              _manifest.get_ntp());
            return;
        }
    }

    do_collect();
}

segment_collector::segment_seq segment_collector::segments() {
    return _segments;
}

void segment_collector::do_collect() {
    auto projected_end_inclusive = _target_end_inclusive.value_or(
      model::offset{});
    if (projected_end_inclusive == model::offset{}) {
        projected_end_inclusive = find_replacement_boundary(_mode);
    }
    // In case of the new upload:
    // - _target_end_inclusive is not set means that the upload is not forced
    //   by the timeout. In this case we need to find the end of the current
    //   segment (if it's sealed).
    // - _target_end_inclusive is set means that the upload is forced by the
    //   timeout. In this case we need to collect segments until the size
    //   limit is reached or the end of the segment is reached.
    auto start = _begin_inclusive;
    vlog(
      archival_log.debug,
      "Segment collect for ntp {} starting at offset {}, upload end "
      "inclusive: {}, last uploaded offset: {}",
      _manifest.get_ntp(),
      start,
      projected_end_inclusive,
      _manifest.get_last_offset());

    // We start with the greedy scan which aims to collect
    // segments until the condition is met. After that we
    // need to check if the last segment is aligned with
    // the manifest segment boundary (only in case of reupload).
    // If not, we need to re-align the end offset to the
    // manifest segment boundary.
    // In case of new segment upload we don't need to do this.
    bool done = false;
    auto can_continue = [&] {
        auto last_collected
          = _segments.empty()
              ? model::offset{}
              : _segments.back()->offsets().get_committed_offset();
        switch (_mode) {
        case segment_collector_mode::compacted_reupload:
        case segment_collector_mode::non_compacted_reupload:
            return last_collected <= _manifest.get_last_offset();
        case segment_collector_mode::new_upload:
            return last_collected < projected_end_inclusive;
        }
    };
    while (!done && can_continue()) {
        // For the new upload mode, we need to find the next segment
        // which is above the _manifest.get_last_offset().
        // Otherwise, we need to find the next segment which is below the
        // _manifest.get_last_offset().
        auto result = find_next_segment(start, _mode);
        if (result.segment.get() == nullptr) {
            break;
        }

        if (unlikely(!_ntp_cfg)) {
            _ntp_cfg = result.ntp_conf;
        }

        auto segment_size = result.segment->size_bytes();
        if (
          _target_end_inclusive.has_value()
          && result.segment->offsets().get_committed_offset()
               >= _target_end_inclusive.value()) {
            // In this case the collected size may overflow
            // _max_uploaded_segment_size a bit so we could actually find
            // _target_end_inclusive inside the last segment.
            vlog(
              archival_log.debug,
              "Segment collect for ntp {} stopping collection, total size: "
              "{} "
              "reached target end offset: {}, current collected size: {}",
              _manifest.get_ntp(),
              _collected_size + segment_size,
              _target_end_inclusive.value(),
              _collected_size);
            // Current segment has to be added to the list of results
            done = true;
        } else if (
          _collected_size > 0
          && _collected_size + segment_size > _max_uploaded_segment_size) {
            // The overflow is allowed in case if the only segment is
            // larger than the limit. Otherwise, if the limit is lower than
            // the size of the segment the uploads will be stalled.
            vlog(
              archival_log.debug,
              "Segment collect for ntp {} stopping collection, total "
              "size: {} will overflow max allowed upload size: {}, current "
              "collected size: {}",
              _manifest.get_ntp(),
              _collected_size + segment_size,
              _max_uploaded_segment_size,
              _collected_size);
            break;
        }

        // For the first segment found, begin offset needs to be
        // re-aligned if it falls inside manifest segment.
        if (
          _segments.empty()
          && _mode == segment_collector_mode::compacted_reupload) {
            // We may have found our first segment, but we can't always use
            // its base offset:
            // - it's possible the log has been prefix truncated within a
            //   segment (e.g. with delete records), so we must bump to the
            //   log start offset
            // - it's possible the segment we found is below our reupload
            //   target start offset (_begin_inclusive), e.g. if the target
            //   start offset is in the middle of a segment.
            _begin_inclusive = std::max(
              {_begin_inclusive,
               _log.offsets().start_offset,
               result.segment->offsets().get_base_offset()});
            align_begin_offset_to_manifest();
        }

        // Only segments from the same term can be concatenated together.
        if (
          !_segments.empty()
          && _segments.back()->offsets().get_term()
               != result.segment->offsets().get_term()) {
            archival_log.debug(
              "Segment collect for ntp {} stopping collection, last "
              "segment "
              "term {} is different from current segment term: {}",
              _manifest.get_ntp(),
              _segments.back()->offsets().get_term(),
              result.segment->offsets().get_term());
            break;
        }

        _segments.push_back(result.segment);
        _generations.push_back(result.segment->get_generation_id()());
        _sizes.push_back(result.segment->size_bytes());
        start = model::next_offset(
          _segments.back()->offsets().get_committed_offset());
        _collected_size += segment_size;
    }

    if (_segments.empty()) {
        // Nothing is collected
        return;
    }

    auto last_collected = _segments.back()->offsets().get_committed_offset();
    if (last_collected >= projected_end_inclusive) {
        _can_replace_manifest_segment = true;
    }

    if (is_reupload_mode(_mode)) {
        align_end_offset_to_manifest(
          _target_end_inclusive.value_or(last_collected));
    } else {
        // In case of new upload we want to end at the end of the segment
        // or at LSO (which is passed through the _target_end_inclusive).
        _end_inclusive = std::min(
          _target_end_inclusive.value_or(last_collected), last_collected);
    }
}

model::offset segment_collector::find_replacement_boundary(
  segment_collector_mode mode) const {
    if (is_reupload_mode(mode)) {
        auto it = _manifest.segment_containing(_begin_inclusive);

        // Crossing this boundary means that the collection can replace at least
        // one segment in manifest.
        model::offset replace_boundary;

        // manifest: 10-19, 25-29
        // _begin_inclusive (in gap): 22.
        auto manifest_end = _manifest.end();
        if (it == manifest_end) {
            // first segment after gap: 25-29
            for (it = _manifest.begin(); it != manifest_end; ++it) {
                const auto& entry = *it;
                if (entry.base_offset > _begin_inclusive) {
                    break;
                }
            }
            // The collection is valid if it can reach the end of the gap: 24
            vassert(it != manifest_end, "Trying to dereference end iterator");
            replace_boundary = it->base_offset - model::offset{1};
        } else {
            replace_boundary = it->committed_offset;
        }

        return replace_boundary;
    }
    // Find new segment boundary for non-reupload mode.
    // If the _target_end_inclusive is set then use it as the boundary.
    // In this case the _target_end_inclusive is seeded with the current LSO.
    // Otherwise, find the boundary based on the size of the segments.
    // The fast path: just return the current segment's committed offset.
    vassert(
      !_target_end_inclusive.has_value(),
      "Target end offset is already defined");
    // Align end offset to the nearest segment.
    auto segment = lower_bound(_begin_inclusive, mode);
    if (segment == nullptr) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {}: can't find segment with base_offset={}",
          _manifest.get_ntp(),
          _begin_inclusive);
        return model::offset{};
    }
    // Otherwise just mimic current behavior by returning the next
    // sealed segment.
    if (segment->has_appender()) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {}: segment not sealed",
          _manifest.get_ntp());
        // Segment is not sealed so we can't upload it
        return model::offset{};
    }
    vlog(
      archival_log.debug,
      "Segment collect for ntp {}: found segment {}",
      _manifest.get_ntp(),
      *segment);
    return segment->offsets().get_committed_offset();
}

void segment_collector::align_end_offset_to_manifest(
  model::offset segment_end) {
    if (segment_end == _manifest.get_last_offset()) {
        _end_inclusive = _manifest.get_last_offset();
    } else if (segment_end > _manifest.get_last_offset()) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {} offset {} advanced "
          "ahead of manifest, clamping to {}",
          _manifest.get_ntp(),
          segment_end,
          _manifest.get_last_offset());
        _end_inclusive = _manifest.get_last_offset();
    } else {
        // Align the end offset to the nearest segment ending in manifest.
        auto it = _manifest.segment_containing(segment_end);
        if (it == _manifest.end()) {
            // segment_end is in a gap in the manifest.
            if (segment_end >= _manifest.get_start_offset().value()) {
                vlog(
                  archival_log.debug,
                  "Segment collect for ntp {}: collection ended at "
                  "gap in manifest: {}",
                  _manifest.get_ntp(),
                  segment_end);

                // try to fill the manifest gap with the data locally
                // available.
                _end_inclusive = segment_end;
            }
            return;
        }

        // If the segment end is not aligned to manifest segment, then
        // pull back to the end of the previous segment.
        if (it->committed_offset == segment_end) {
            _end_inclusive = segment_end;
        } else {
            _end_inclusive = it->base_offset - model::offset{1};
        }
    }
}

ss::lw_shared_ptr<storage::segment> segment_collector::lower_bound(
  model::offset offset, segment_collector_mode mode) const {
    const auto& segment_set = _log.segments();
    auto it = segment_set.lower_bound(offset);
    if (
      it == _log.segments().end() && !is_reupload_mode(mode)
      && _begin_inclusive < _log.offsets().committed_offset) {
        vlog(
          archival_log.warn,
          "Segment collect for {}: can't find segment with base_offset={}",
          _manifest.get_ntp(),
          _begin_inclusive);

        it = std::find_if(
          _log.segments().begin(),
          _log.segments().end(),
          [this](const ss::lw_shared_ptr<storage::segment>& s) {
              return s->offsets().get_base_offset() >= _begin_inclusive;
          });
    }

    if (it == segment_set.end()) {
        vlog(
          archival_log.debug,
          "Finding next segment for {}: can't find segment after "
          "offset: {}",
          _manifest.get_ntp(),
          offset);
        return nullptr;
    }
    return *it;
}

segment_collector::lookup_result segment_collector::find_next_segment(
  model::offset start_offset, segment_collector_mode mode) {
    // 'start_offset' should always be above the start offset of the local
    // log as we skip to it in the calling code (`collect_segments`).
    if (start_offset < _log.offsets().start_offset) {
        vlog(
          archival_log.warn,
          "Finding next segment for {}: can't find segments below the "
          "local "
          "log start offset ({} < {})",
          _manifest.get_ntp(),
          start_offset,
          _log.offsets().start_offset);
        return {};
    }

    auto segment = lower_bound(start_offset, mode);
    if (segment == nullptr) {
        vlog(
          archival_log.debug,
          "Finding next segment for {}: can't find segment with base_offset={}",
          _manifest.get_ntp(),
          start_offset);
        return {};
    }

    auto closed = !segment->has_appender();

    if (!is_reupload_mode(mode) && !closed) {
        if (!_target_end_inclusive.has_value()) {
            // The segment is not sealed and we don't know the LSO so
            // we can't upload it.
            vlog(
              archival_log.debug,
              "Finding next segment for {}: segment {} not sealed",
              _manifest.get_ntp(),
              segment);
            return {};
        }
        auto committed = segment->offsets().get_committed_offset();
        auto end_inclusive = std::min(
          _target_end_inclusive.value_or(committed), committed);
        auto below_flush_offset = _flush_offset.has_value()
                                  && segment->offsets().get_base_offset()
                                       <= _flush_offset.value();
        if (!below_flush_offset) {
            auto kafka_start_offset = _log.from_log_offset(_begin_inclusive);
            auto kafka_lso = _log.from_log_offset(
              model::next_offset(end_inclusive));

            if (kafka_start_offset >= kafka_lso) {
                // If timeboxed uploads are enabled and there is no producer
                // activity, we can get into a nasty loop where we upload a
                // segment, add an archival metadata batch, upload a segment
                // containing that batch, add another archival metadata batch,
                // etc. This leads to lots of small segments that don't contain
                // data being uploaded. To avoid it, we check that kafka
                // (translated) offset increases.
                vlog(
                  archival_log.debug,
                  "Segment collector for {}: can't find candidate, only "
                  "non-data "
                  "batches to upload (kafka start_offset: {}, kafka "
                  "last_stable_offset: {})",
                  _manifest.get_ntp(),
                  kafka_start_offset,
                  kafka_lso);
                return {};
            }
        }
    }

    auto dirty_offset = segment->offsets().get_dirty_offset();
    if (
      !is_reupload_mode(mode) && _end_exclusive.has_value()
      && dirty_offset >= _end_exclusive && !_target_end_inclusive.has_value()) {
        vlog(
          archival_log.debug,
          "Segment collector for {}: can't find candidate, candidate dirty "
          "offset {} is above last_stable_offset {}",
          _manifest.get_ntp(),
          dirty_offset,
          model::prev_offset(_end_exclusive.value()));
        return {};
    }

    auto segment_is_compacted = eligible_for_compacted_reupload(*segment);
    auto compacted_segment_expected
      = mode == segment_collector_mode::compacted_reupload;
    auto compacted_segment_allowed
      = mode != segment_collector_mode::non_compacted_reupload;

    if (
      segment_is_compacted == compacted_segment_expected
      || (segment_is_compacted && compacted_segment_allowed)) {
        vlog(
          archival_log.trace,
          "Found segment for ntp {}: {}",
          _manifest.get_ntp(),
          segment);
        return {.segment = segment, .ntp_conf = &_log.config()};
    }
    vlog(
      archival_log.debug,
      "Finding next segment for {}: no "
      "segments after offset: {}",
      _manifest.get_ntp(),
      start_offset);
    return {};
}

model::offset segment_collector::begin_inclusive() const {
    return _begin_inclusive;
}

model::offset segment_collector::end_inclusive() const {
    return _end_inclusive;
}

const storage::ntp_config* segment_collector::ntp_cfg() const {
    return _ntp_cfg;
}

bool segment_collector::should_replace_manifest_segment() const {
    return _can_replace_manifest_segment && _begin_inclusive < _end_inclusive;
}

bool segment_collector::segment_ready_for_upload() const {
    return _begin_inclusive <= _end_inclusive && !_segments.empty();
}

cloud_storage::segment_name segment_collector::adjust_segment_name() const {
    vassert(
      !_segments.empty(), "Cannot calculate segment name with no segments");

    auto first = _segments.front();
    auto file_name = first->filename();
    auto meta = storage::segment_path::parse_segment_filename(file_name);
    auto version = meta ? meta->version : storage::record_version_type::v1;

    cloud_storage::segment_name name{};
    if (_begin_inclusive == first->offsets().get_base_offset()) {
        auto orig_path = std::filesystem::path(file_name);
        name = cloud_storage::segment_name(orig_path.filename().string());
        vlog(archival_log.debug, "Using original segment name: {}", name);
    } else {
        auto path = storage::segment_path::make_segment_path(
          *_ntp_cfg, _begin_inclusive, first->offsets().get_term(), version);
        name = cloud_storage::segment_name(path.filename().string());
        vlog(archival_log.debug, "Using adjusted segment name: {}", name);
    }

    return name;
}

void segment_collector::align_begin_offset_to_manifest() {
    if (_begin_inclusive >= _manifest.get_last_offset()) {
        return;
    }

    if (_begin_inclusive < _manifest.get_start_offset().value()) {
        vlog(
          archival_log.debug,
          "_begin_inclusive is behind manifest for ntp: {}, skipping "
          "forward "
          "to "
          "start of manifest from: {} to: {}",
          _manifest.get_ntp(),
          _begin_inclusive,
          _manifest.get_start_offset().value());

        // manifest: 10-40
        // _begin_inclusive: before: 5, after: 10
        _begin_inclusive = _manifest.get_start_offset().value();
        return;
    }

    auto it = _manifest.find(_begin_inclusive);
    auto end_it = _manifest.end();
    // If iterator points to a segment, it means that _begin_inclusive is
    // aligned on manifest segment boundary, so do nothing. Otherwise, skip
    // _begin_inclusive to the start of the next manifest segment.
    if (it == end_it) {
        it = _manifest.segment_containing(_begin_inclusive);

        // manifest: 10-19, 25-29
        // _begin_inclusive (in gap): before: 22, after: 22
        if (it == end_it) {
            vlog(
              archival_log.debug,
              "_begin_inclusive lies in manifest gap for ntp: {} "
              "value: {}",
              _manifest.get_ntp(),
              _begin_inclusive);
            return;
        }

        // manifest: 10-19, 20-29
        // _begin_inclusive: before: 15, after: 20 OR
        // _begin_inclusive: before: 25, after: 30
        _begin_inclusive = it->committed_offset + model::offset{1};
        vlog(
          archival_log.debug,
          "_begin_inclusive skipped to start of next segment for ntp: {} "
          "to: {}",
          _manifest.get_ntp(),
          _begin_inclusive);
    }
}

ss::future<candidate_creation_result> segment_collector::make_upload_candidate(
  ss::lowres_clock::duration segment_lock_duration) {
    if (_segments.empty()) {
        vlog(
          archival_log.debug,
          "No segments to reupload for {}",
          _manifest.get_ntp());
        co_return candidate_creation_error::no_segments_collected;
    } else {
        if (archival_log.is_enabled(ss::log_level::debug)) {
            std::stringstream seg;
            for (const auto& s : _segments) {
                fmt::print(
                  seg,
                  "{}-{}/{}; ",
                  s->offsets().get_base_offset(),
                  s->offsets().get_committed_offset(),
                  s->size_bytes());
            }
            vlog(archival_log.debug, "Collected segments: {}", seg.str());
        }
    }

    auto last = _segments.back();
    // Get the size of the last segment collected _before_ acquiring read locks
    // because upload size accounting has already taken place at this point.
    auto last_size_bytes = last->size_bytes();

    // Take the locks before opening any readers on the segments.
    auto deadline = std::chrono::steady_clock::now() + segment_lock_duration;
    std::vector<ss::future<ss::rwlock::holder>> locks;
    locks.reserve(_segments.size());
    std::transform(
      _segments.begin(),
      _segments.end(),
      std::back_inserter(locks),
      [&deadline](auto& seg) { return seg->read_lock(deadline); });

    auto locks_resolved = co_await ss::when_all_succeed(
      locks.begin(), locks.end());

    std::vector<uint64_t> current_gen;
    current_gen.reserve(_segments.size());
    std::transform(
      _segments.begin(),
      _segments.end(),
      std::back_inserter(current_gen),
      [](const auto& seg) { return seg->get_generation_id()(); });

    auto last_unsealed = last->has_appender();

    // special case: skip the generation check iff
    //   - the last segment in the list was NOT closed AND
    //   - the last segment is the ONLY segment with gen ID difference

    vassert(
      _generations.size() == current_gen.size(),
      "Cached generations should match size of accumulated segments ({} vs {})",
      _generations.size(),
      current_gen.size());

    auto gen_id_diffs = std::views::iota(0ul, _segments.size())
                        | std::views::filter([&](auto i) -> bool {
                              return _generations.at(i) != current_gen.at(i);
                          })
                        | std::ranges::to<chunked_vector<size_t>>();
    auto skip_gen_id_check = last_unsealed && gen_id_diffs.size() == 1
                             && _generations.back() != current_gen.back();

    // If the last collected segment is not closed,
    if (!skip_gen_id_check && !gen_id_diffs.empty()) {
        std::stringstream sstr;
        for (auto i : gen_id_diffs) {
            // Segment was updated concurrently while we were waiting
            // for the locks.
            fmt::print(
              sstr,
              "segment {}-{} (seq: {}, old gen: {}, new gen: {}, old size: "
              "{}, new size: {}); ",
              _segments.at(i)->offsets().get_base_offset(),
              _segments.at(i)->offsets().get_committed_offset(),
              i,
              _generations.at(i),
              current_gen.at(i),
              _sizes.at(i),
              _segments.at(i)->size_bytes());
        }
        // One or more segments were updated while we were waiting for the
        // locks. It's a race condition so we should fail the upload and retry.
        vlog(
          archival_log.info,
          "Segment generation mismatch for {}: {}",
          _manifest.get_ntp(),
          sstr.str());
        co_return candidate_creation_error::concurrency_error;
    }

    auto first = _segments.front();
    auto head_seek_result = co_await storage::convert_begin_offset_to_file_pos(
      _begin_inclusive, first, first->index().base_timestamp());

    if (head_seek_result.has_error()) {
        co_return candidate_creation_error::begin_offset_seek_error;
    }

    auto tail_seek_result = co_await storage::convert_end_offset_to_file_pos(
      _end_inclusive, last, last->index().max_timestamp());

    if (tail_seek_result.has_error()) {
        co_return candidate_creation_error::end_offset_seek_error;
    }

    auto head_seek = head_seek_result.value();
    auto tail_seek = tail_seek_result.value();

    if (head_seek.offset_inside_batch || tail_seek.offset_inside_batch) {
        vlog(
          archival_log.warn,
          "The upload candidate boundaries lie inside batch, skipping "
          "upload. "
          "begin inclusive: {}, is inside batch: {}, seek result: {}, end "
          "inclusive: {}, is "
          "inside batch: {}, seek result: {}",
          _begin_inclusive,
          head_seek.offset_inside_batch,
          head_seek.offset,
          _end_inclusive,
          tail_seek.offset_inside_batch,
          tail_seek.offset);
        if (is_reupload_mode(_mode)) {
            co_return skip_offset_range{
              .begin_offset = _begin_inclusive,
              .end_offset = _end_inclusive,
              .reason = candidate_creation_error::offset_inside_batch};
        } else {
            // This should never occur, but return an error here just in case.
            co_return candidate_creation_error::offset_inside_batch;
        }
    }

    vlog(
      archival_log.debug,
      "collected size: {}, last segment {}-{}/{}, head seek bytes: {}, "
      "tail "
      "seek bytes: {}",
      _collected_size,
      last->offsets().get_base_offset(),
      last->offsets().get_committed_offset(),
      last_size_bytes,
      head_seek.bytes,
      tail_seek.bytes);

    size_t content_length = _collected_size
                            - (head_seek.bytes + last_size_bytes);
    content_length += tail_seek.bytes;

    auto starting_offset = head_seek.offset;
    if (starting_offset != _begin_inclusive) {
        vlog(
          archival_log.debug,
          "adjusting begin offset of upload candidate from {} to {}",
          starting_offset,
          _begin_inclusive);
        starting_offset = _begin_inclusive;
    }

    auto final_offset = tail_seek.offset;
    if (final_offset != _end_inclusive) {
        vlog(
          archival_log.debug,
          "adjusting end offset of upload candidate from {} to {}",
          final_offset,
          _end_inclusive);
        final_offset = _end_inclusive;
    }

    // Now that we know the final size of the reupload, perform
    // a final sanity check to ensure that the size of the new segment
    // is smaller than that of the replaced one. Skip the upload if that's
    // not the case.
    if (
      auto to_replace = _manifest.find(starting_offset);
      to_replace != _manifest.end()) {
        if (
          is_reupload_mode(_mode)
          && to_replace->committed_offset == final_offset
          && to_replace->size_bytes <= content_length) {
            vlog(
              archival_log.debug,
              "Skipping re-upload of compacted segment as its size has "
              "not decreased as a result of self-compaction: {}",
              _segments.front());

            co_return skip_offset_range{
              .begin_offset = _begin_inclusive,
              .end_offset = _end_inclusive,
              .reason = candidate_creation_error::upload_size_unchanged};
        }
        if (!is_reupload_mode(_mode)) {
            vlog(
              archival_log.debug,
              "New segment upload already appears in manifest: {}",
              _segments.front());
            co_return candidate_creation_error::concurrency_error;
        }
    }

    co_return upload_candidate_with_locks{
      upload_candidate{
        .exposed_name = adjust_segment_name(),
        .starting_offset = starting_offset,
        .file_offset = head_seek.bytes,
        .content_length = content_length,
        .final_offset = final_offset,
        .final_file_offset = tail_seek.bytes,
        .base_timestamp = head_seek.ts,
        .max_timestamp = tail_seek.ts,
        .term = first->offsets().get_term(),
        .sources = _segments,
      },
      std::move(locks_resolved)};
}
ss::future<segment_collector_stream_result>
segment_collector::make_upload_candidate_stream(
  ss::lowres_clock::duration segment_lock_duration) {
    auto candidate_res = co_await make_upload_candidate(segment_lock_duration);

    vassert(
      !std::holds_alternative<std::monostate>(candidate_res),
      "Unexpected default upload candidate creation result");

    if (std::holds_alternative<candidate_creation_error>(candidate_res)) {
        auto err = std::get<candidate_creation_error>(candidate_res);
        vlog(archival_log.warn, "Candidate creation error: {}", err);
        co_return err;
    } else if (std::holds_alternative<skip_offset_range>(candidate_res)) {
        auto skip = std::get<skip_offset_range>(candidate_res);
        vlog(
          archival_log.debug,
          "Skipping offset range: {}-{}, reason: {}",
          skip.begin_offset,
          skip.end_offset,
          skip.reason);
        co_return skip;
    }

    auto& cand_with_locks = std::get<upload_candidate_with_locks>(
      candidate_res);

    auto& cand = cand_with_locks.candidate;
    auto front = cand.sources.front();

    segment_collector_stream stream;
    stream.start_offset = cand.starting_offset;
    stream.end_offset = cand.final_offset;
    stream.min_timestamp = cand.base_timestamp;
    stream.max_timestamp = cand.max_timestamp;
    stream.size = cand.content_length;
    stream.is_compacted = front->is_compacted_segment()
                          && eligible_for_compacted_reupload(*front);
    stream.term = cand.term;
    stream.create_input_stream =
      [segments = cand.sources,
       locks = std::move(cand_with_locks.read_locks),
       file_offset = cand.file_offset,
       final_file_offset
       = cand.final_file_offset]() mutable -> ss::input_stream<char> {
        storage::concat_segment_reader_view crv(
          segments, file_offset, final_file_offset);
        return crv.take_stream();
    };
    co_return stream;
}
size_t segment_collector::collected_size() const { return _collected_size; }

} // namespace archival
