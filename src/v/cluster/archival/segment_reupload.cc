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
#include "cluster/partition.h"
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
#include <seastar/core/scheduling.hh>
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

std::ostream& operator<<(std::ostream& os, segment_collector_mode m) {
    switch (m) {
    case segment_collector_mode::compacted_reupload:
        return os << "segment_collector_mode::compacted_reupload";
    case segment_collector_mode::non_compacted_reupload:
        return os << "segment_collector_mode::non_compacted_reupload";
    case segment_collector_mode::new_upload:
        return os << "segment_collector_mode::new_upload";
    case segment_collector_mode::compacted_reupload_v2:
        return os << "segment_collector_mode::compacted_reupload_v2";
    case segment_collector_mode::non_compacted_reupload_v2:
        return os << "segment_collector_mode::non_compacted_reupload_v2";
    case segment_collector_mode::new_upload_v2:
        return os << "segment_collector_mode::new_upload_v2";
    }
}

std::ostream& operator<<(std::ostream& s, const upload_candidate& c) {
    if (c.content_length == 0) {
        return s << "{empty}";
    }
    std::vector<ss::sstring> source_names;
    source_names.reserve(c.sources.size() + c.remote_sources.size());
    std::ranges::transform(
      c.sources, std::back_inserter(source_names), [](const auto& src) {
          return src->filename();
      });
    std::ranges::transform(
      c.remote_sources, std::back_inserter(source_names), [](const auto& src) {
          return src().native();
      });

    fmt::print(
      s,
      "{{exposed_name: {}, starting_offset: {}, "
      "file_offset: {}, content_length: {}, final_offset: {}, "
      "final_file_offset: {}, term: {}, source names: {}}}",
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
      skip_range.start_offset,
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
    switch (mode) {
    case segment_collector_mode::compacted_reupload:
    case segment_collector_mode::non_compacted_reupload:
    case segment_collector_mode::compacted_reupload_v2:
    case segment_collector_mode::non_compacted_reupload_v2:
        return true;
    case segment_collector_mode::new_upload:
    case segment_collector_mode::new_upload_v2:
        return false;
    }
}
[[maybe_unused]] static bool
is_compacted_reupload_mode(segment_collector_mode mode) {
    return mode == segment_collector_mode::compacted_reupload
           || mode == segment_collector_mode::compacted_reupload_v2;
}

[[maybe_unused]] static bool
is_non_compacted_reupload_mode(segment_collector_mode mode) {
    return mode == segment_collector_mode::non_compacted_reupload
           || mode == segment_collector_mode::non_compacted_reupload_v2;
}

bool is_v2_mode(segment_collector_mode mode) {
    switch (mode) {
    case segment_collector_mode::compacted_reupload:
    case segment_collector_mode::non_compacted_reupload:
    case segment_collector_mode::new_upload:
        return false;
    case segment_collector_mode::compacted_reupload_v2:
    case segment_collector_mode::non_compacted_reupload_v2:
    case segment_collector_mode::new_upload_v2:
        return true;
    }
}

[[maybe_unused]] bool is_v1_mode(segment_collector_mode mode) {
    return !is_v2_mode(mode);
}

std::tuple<size_t, size_t>
upload_size_jitter(segment_collector_mode mode, size_t sz) {
    // for non-compacted reupload, we know the exact size. any deviation from
    // that is an error, so don't apply any jitter here.
    if (is_non_compacted_reupload_mode(mode)) {
        return std::make_tuple(sz, sz);
    }
    if (is_compacted_reupload_mode(mode)) {
        return std::make_tuple(0, sz);
    }
    // primarily for testing purposes, where we may not be able to predict
    // precise upload size but also want some control over the range
    if (sz < 1_MiB) {
        return std::make_tuple(sz / 2, sz);
    }
    // We want to guarantee that there is always some spread and
    // that its width is limited to some reasonable value. This
    // implementation will try to keep it in 2-4KiB range for
    // small segments (<20MiB) and 2-4MiB range for large segments.
    fast_prng rng;
    size_t min_size = 0, max_size = 0;
    if (sz < 20_MiB) {
        min_size = sz - 1_KiB - (rng() & (1_KiB - 1));
        max_size = sz - 1_KiB - (rng() & (1_KiB - 1));
    } else {
        min_size = sz - 1_MiB - (rng() & (1_MiB - 1));
        max_size = sz - 1_MiB - (rng() & (1_MiB - 1));
    }
    if (min_size > max_size) {
        std::swap(min_size, max_size);
    }
    return std::make_tuple(min_size, max_size);
}

} // namespace

bool segment_collector::collect_segments() {
    if (_manifest.size() == 0 && is_reupload_mode(_mode)) {
        vlog(
          archival_log.debug,
          "No segments to collect for ntp {}, manifest empty",
          _manifest.get_ntp());
        return false;
    }

    // start_offset < log start due to eviction of segments before
    // they could be uploaded, skip forward to log start.
    if (_begin_inclusive < _log.offsets().start_offset) {
        switch (_mode) {
        case segment_collector_mode::new_upload:
        case segment_collector_mode::new_upload_v2:
        case segment_collector_mode::compacted_reupload:
        case segment_collector_mode::compacted_reupload_v2:
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
        case segment_collector_mode::non_compacted_reupload_v2:
            vlog(
              archival_log.debug,
              "Provided start offset is below the start offset of the local "
              "log: "
              "{} < {} for ntp {}. Exiting early.",
              _begin_inclusive,
              _log.offsets().start_offset,
              _manifest.get_ntp());
            return false;
        }
    }

    // Handle begin offset alignment to manifest segment boundary (if
    // required).
    switch (_mode) {
    case segment_collector_mode::compacted_reupload:
    case segment_collector_mode::compacted_reupload_v2:
        align_begin_offset_to_manifest();
        break;
    case segment_collector_mode::non_compacted_reupload:
    case segment_collector_mode::non_compacted_reupload_v2:
        if (_manifest.find(_begin_inclusive) == _manifest.end()) {
            vlog(
              archival_log.debug,
              "Provided start offset {} is not aligned to a segment in the "
              "manifest: for ntp {}. Exiting early.",
              _begin_inclusive,
              _manifest.get_ntp());
            return false;
        }
        break;
    case segment_collector_mode::new_upload:
    case segment_collector_mode::new_upload_v2:
        if (_manifest.get_last_offset() > _begin_inclusive) {
            vlog(
              archival_log.debug,
              "Provided start offset {} is behind the manifest last offset "
              "{}: for ntp {}. Exiting early.",
              _begin_inclusive,
              _manifest.get_last_offset(),
              _manifest.get_ntp());
            return false;
        }
        break;
    }

    if (
      is_reupload_mode(_mode)
      && _begin_inclusive >= _manifest.get_last_offset()) {
        vlog(
          archival_log.warn,
          "Start offset {} is ahead of manifest last offset {} for ntp {}, not "
          "a reupload",
          _begin_inclusive,
          _manifest.get_last_offset(),
          _manifest.get_ntp());
        return false;
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
            return false;
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
            return false;
        }
    }

    if (is_v1_mode(_mode)) {
        do_collect();
        return is_reupload_mode(_mode) ? should_replace_manifest_segment()
                                       : segment_ready_for_upload();
    }

    if (is_reupload_mode(_mode)) {
        // TODO(oren): jank
        _can_replace_manifest_segment = do_reupload_collect_v2();
        return should_replace_manifest_segment();
    }
    return do_collect_v2();
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
        case segment_collector_mode::compacted_reupload_v2:
        case segment_collector_mode::non_compacted_reupload_v2:
        case segment_collector_mode::new_upload_v2:
            vassert(
              false,
              "Incorrect use of v2 collector mode with v1 collect: {}",
              _mode);
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
            vlog(
              archival_log.debug,
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
        _end_inclusive = align_end_offset_to_manifest(
                           _target_end_inclusive.value_or(last_collected))
                           .value_or(_end_inclusive);
    } else {
        // In case of new upload we want to end at the end of the segment
        // or at LSO (which is passed through the _target_end_inclusive).
        _end_inclusive = std::min(
          _target_end_inclusive.value_or(last_collected), last_collected);
    }
}

bool segment_collector::do_collect_v2() {
    vassert(
      !is_reupload_mode(_mode), "do_collect_v2 for new segment mode only");
    vassert(_end_exclusive.has_value(), "Expected LSO for new segment mode");
    if (auto maybe_adjusted_start = _log.base_offset_lower_bound(
          _begin_inclusive);
        maybe_adjusted_start.has_value()) {
        _begin_inclusive = std::max(
          _begin_inclusive, maybe_adjusted_start.value());
    }
    if (_begin_inclusive >= _end_exclusive.value()) {
        vlog(
          archival_log.debug,
          "{}: Start offset {} is above committed offset {}, nothing to "
          "upload",
          _manifest.get_ntp(),
          _begin_inclusive,
          model::prev_offset(_end_exclusive.value()));
        return false;
    }

    _end_inclusive = std::min(
      _target_end_inclusive.value_or(model::offset::max()),
      model::prev_offset(_end_exclusive.value()));

    auto need_flush = _begin_inclusive
                      <= _flush_offset.value_or(model::offset::min());

    if (!need_flush) {
        // If timeboxed uploads are enabled and there is no producer
        // activity, we can get into a nasty loop where we upload a
        // segment, add an archival metadata batch, upload a segment
        // containing that batch, add another archival metadata batch,
        // etc. This leads to lots of small segments that don't contain
        // data being uploaded. To avoid it, we check that kafka
        // (translated) offset increases.
        auto kafka_start_offset = _log.from_log_offset(_begin_inclusive);
        auto kafka_lso = _log.from_log_offset(
          model::next_offset(_end_inclusive));
        if (kafka_start_offset >= kafka_lso) {
            vlog(
              archival_log.debug,
              "do_collect_v2 for {}: can't find candidate, only "
              "non-data "
              "batches to upload (kafka start_offset: {}, kafka "
              "last_stable_offset: {}) reupload?: {}",
              _manifest.get_ntp(),
              kafka_start_offset,
              kafka_lso,
              is_reupload_mode(_mode));
            return false;
        }
    }

    vlog(
      archival_log.debug,
      "do_collect_v2 for {}: begin_inclusive: {} end_inclusive: {}",
      _manifest.get_ntp(),
      _begin_inclusive,
      _end_inclusive);

    return _begin_inclusive <= _end_inclusive;
}

bool segment_collector::do_reupload_collect_v2() {
    vassert(
      is_reupload_mode(_mode),
      "do_reupload_collect_v2 is for reupload modes only");

    // NOTE(oren): Consider
    //   - non-compacted reupload will _always_ have set a target_end_inclusive
    //   - compacted reupload will _never_ have one
    //   - maybe a builder interface would be a bit more clear here...

    auto projected_end_inclusive = _target_end_inclusive.value_or(
      find_replacement_boundary(_mode));

    std::optional<model::offset> last_compacted_offset{};
    if (is_compacted_reupload_mode(_mode)) {
        last_compacted_offset = _log.max_compacted_offset(_begin_inclusive);

        if (!last_compacted_offset.has_value()) {
            vlog(
              archival_log.debug,
              "{} No compacted offsets found beyond {}",
              _manifest.get_ntp(),
              _begin_inclusive);
            // TODO(oren): mega jank
            _end_inclusive = model::prev_offset(_begin_inclusive);
            return false;
        }
    }

    auto end_offset = last_compacted_offset.value_or(projected_end_inclusive);

    _end_inclusive
      = align_end_offset_to_manifest(end_offset).value_or(_end_inclusive);

    return _end_inclusive >= projected_end_inclusive;
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
        if (it == _manifest.end()) {
            // first segment after gap: 25-29
            for (it = _manifest.begin(); it != _manifest.end(); ++it) {
                const auto& entry = *it;
                if (entry.base_offset > _begin_inclusive) {
                    break;
                }
            }
            // The collection is valid if it can reach the end of the gap: 24
            vassert(
              it != _manifest.end(), "Trying to dereference end iterator");
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

std::optional<model::offset>
segment_collector::align_end_offset_to_manifest(model::offset end_offset) {
    if (end_offset == _manifest.get_last_offset()) {
        return _manifest.get_last_offset();
    }
    if (end_offset > _manifest.get_last_offset()) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {} offset {} advanced "
          "ahead of manifest, clamping to {}",
          _manifest.get_ntp(),
          end_offset,
          _manifest.get_last_offset());
        return _manifest.get_last_offset();
    }
    // Align the end offset to the nearest segment ending in manifest.
    auto it = _manifest.segment_containing(end_offset);
    if (it != _manifest.end()) {
        // If the end offset is aligned to the manifest segment:
        //   - return end offset
        //   - otherwise, pull back to the end of the previous manifest segment
        return it->committed_offset == end_offset
                 ? end_offset
                 : model::prev_offset(it->base_offset);
    }

    // end_offset is in a gap in the manifest.
    if (end_offset >= _manifest.get_start_offset().value()) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {}: collection ended at "
          "gap in manifest: {}",
          _manifest.get_ntp(),
          end_offset);

        // try to fill the manifest gap with the data locally
        // available.
        return end_offset;
    }
    // TODO(oren): what is the meaning of this? should we just return the input
    // value?
    return std::nullopt;
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

    // If iterator points to a segment, it means that _begin_inclusive is
    // aligned on manifest segment boundary, so do nothing. Otherwise, skip
    // _begin_inclusive to the start of the next manifest segment.
    if (it == _manifest.end()) {
        it = _manifest.segment_containing(_begin_inclusive);

        // manifest: 10-19, 25-29
        // _begin_inclusive (in gap): before: 22, after: 22
        if (it == _manifest.end()) {
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
    auto last_size_bytes = last->size_bytes();
    auto last_unsealed = last->has_appender();

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

    // special case: skip the generation check iff
    //   - the last segment in the list was NOT closed AND
    //   - the last segment is the ONLY segment with gen ID difference

    auto gen_id_diff_view = boost::irange(0ul, _segments.size())
                            | std::views::filter([this, &current_gen](auto i) {
                                  return _generations.at(i)
                                         != current_gen.at(i);
                              });

    auto skip_gen_id_check = std::accumulate(
      gen_id_diff_view.begin(),
      gen_id_diff_view.end(),
      last_unsealed && !gen_id_diff_view.empty(),
      std::logical_and{});

    // If the last collected segment is not closed,
    if (!skip_gen_id_check && !gen_id_diff_view.empty()) {
        std::stringstream sstr;
        for (auto i : gen_id_diff_view) {
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
        // The segment was updated while we were waiting for the locks.
        // It's a race condition so we should fail the upload and retry.
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
              .start_offset = _begin_inclusive,
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

    // This adjustment is only ever relevant during compacted reupload, where
    // the boundary offset of the target range may have been compacted away.
    // For new segment uploads, this condition means there's a gap in the actual
    // log. We need to pass a valid offset range to 'async_data_uploader', so we
    // shouldn't perform the start_offset adjustment in this case.
    // See 4253766df74ca8ee53e17702655976c30298ebea for more detail
    auto is_compacted = first->is_compacted_segment()
                        && eligible_for_compacted_reupload(*first);
    if (starting_offset != _begin_inclusive && is_compacted) {
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
    if (auto to_replace = _manifest.find(starting_offset);
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
              .start_offset = _begin_inclusive,
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
          skip.start_offset,
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
          std::move(segments), file_offset, final_file_offset);
        return crv.take_stream();
    };
    co_return stream;
}

ss::future<candidate_creation_result>
segment_collector::make_segment_upload_candidate(
  cluster::partition& parent,
  ss::lowres_clock::duration segment_lock_duration) {
    vassert(is_v2_mode(_mode), "Unsupported mode: {}", _mode);

    // TODO(oren): Perhaps for compacted reuploads (or reuploads generally), we
    // should have a totally distinct candidate creation function. For compacted
    // reuploads, e.g., we can't possibly know the end offset ahead of time
    // since we always scroll to the last compacted offset. Does it make more
    // sense to do the size capped upload _first_, then align to a manifest
    // segment end and return that? This way in the best case (i.e. when we can
    // fit _everything_), we only do one segment creation. This would be much
    // much better I think.

    auto [min_size, max_size] = upload_size_jitter(
      _mode, _max_uploaded_segment_size);

    vlog(
      archival_log.debug,
      "start collecting segments, start offset {}, min size {}, target "
      "size {}, max_size {}, mode: {}",
      _begin_inclusive,
      min_size,
      _max_uploaded_segment_size,
      max_size,
      _mode);

    auto base_offset = _begin_inclusive;
    auto committed_offset = _end_inclusive;
    auto base_term = parent.get_term(base_offset);
    auto committed_term = parent.get_term(committed_offset);

    if (base_offset > committed_offset) {
        vlog(
          archival_log.debug,
          "{} Base offset {} exceeds committed offset {}, nothing to do",
          _manifest.get_ntp(),
          base_offset,
          committed_offset);
        co_return candidate_creation_error::no_segments_collected;
    }

    if (base_term == model::term_id{}) {
        vlog(
          archival_log.debug,
          "{} Base offsfet {} not found in local log",
          _manifest.get_ntp(),
          base_offset);
        co_return candidate_creation_error::no_segment_for_begin_offset;
    }

    bool force_upload = _target_end_inclusive.has_value();
    vlog(
      archival_log.debug,
      "base offset: {}, base term: {}, committed offset: {}, committed term:"
      "{} - force upload {}",
      base_offset,
      base_term,
      committed_offset,
      committed_term,
      force_upload);

    if (base_term != committed_term) {
        // there's a term change in the range we want, so we should upload even
        // if the result is not as large as we like. we'll never construct a
        // larger upload than that, and we might be able to merge w/ the
        // previous segment at some point.
        force_upload = true;
        auto term_last_offset = parent.get_term_last_offset(base_term);
        if (!term_last_offset.has_value()) {
            co_return candidate_creation_error::end_offset_seek_error;
        }
        committed_offset = model::prev_offset(term_last_offset.value());
        committed_term = parent.get_term(committed_offset);
        vlog(
          archival_log.debug,
          "base offset: {}, base term: {}, adjusted committed offset: {}, "
          "adjusted committed term: {}, force upload? {}",
          base_offset,
          base_term,
          committed_offset,
          committed_term,
          force_upload);
    }
    // sanity check
    if (parent.get_term(base_offset) != parent.get_term(committed_offset)) {
        throw std::runtime_error("base and committed offset mismatch");
    }

    constexpr auto check_compaction =
      [](const segment_upload& upl, segment_collector_mode m) -> bool {
        auto upload_is_compacted = upl.get_meta().is_compacted
                                   && upl.get_meta().compaction_complete;
        auto expect_compacted = is_compacted_reupload_mode(m);
        auto allow_compacted = !is_non_compacted_reupload_mode(m);
        return upload_is_compacted == expect_compacted
               || (upload_is_compacted && allow_compacted);
    };

    auto create_candidate =
      [&parent](const upload_reconciliation_result& meta) -> upload_candidate {
        return upload_candidate{
          // .exposed_name = generated from segment metadata later on
          .starting_offset = meta.offsets.base,
          // .file_offset = don't care
          .content_length = meta.size_bytes,
          .final_offset = meta.offsets.last,
          // .final_file_offset = don't care
          .base_timestamp = meta.base_timestamp,
          .max_timestamp = meta.max_timestamp,
          .term = parent.get_term(meta.offsets.base),
          // .sources = don't care
        };
    };

    auto read_buffer_size
      = config::shard_local_cfg().storage_read_buffer_size();

    auto deadline = ss::lowres_clock::now() + segment_lock_duration;
    inclusive_offset_range range{base_offset, committed_offset};

    // TODO(oren): we don't actually need a stream here. just the metadata. so
    // maybe it's better or easier to have a mode where we don't actually
    // construct the stream. or maybe should just return the fully initialized
    // upload object through the candidate struct.
    auto upl_res = co_await segment_upload::make_segment_upload(
      &parent,
      range,
      read_buffer_size,
      ss::default_scheduling_group(),
      deadline,
      force_upload);

    if (upl_res.has_error()) {
        if (upl_res.error() == archival::error_outcome::offset_in_batch) {
            // TODO(oren): would be nice to transmit which offset did
            vlog(
              archival_log.warn,
              "The upload candidate boundaries lie inside batch, skipping "
              "upload. begin inclusive: {}, end inclusive: {}",
              _begin_inclusive,
              _end_inclusive);
            co_return skip_offset_range{
              .start_offset = _begin_inclusive,
              .end_offset = _end_inclusive,
              .reason = candidate_creation_error::offset_inside_batch,
            };
        }
        // NOTE(oren): timeout may or may not be benign, but under load (many
        // partitions) it's fairly common for read lock acquisition to time out.
        // Needs more investigation, but for now we can let it pass and move on.
        if (
          upl_res.error() != archival::error_outcome::not_enough_data
          && upl_res.error() != archival::error_outcome::timed_out) {
            vlog(
              archival_log.error,
              "Can't find upload candidate for {}: {}",
              range,
              upl_res.error().message());
        }
        co_return candidate_creation_error::no_segments_collected;
    }

    auto upl = std::move(upl_res).value();
    // TODO(oren): would be neat to push this down into segment_upload
    if (!check_compaction(*upl, _mode)) {
        vlog(
          archival_log.debug,
          "{}: No data after offset {} in {}",
          _manifest.get_ntp(),
          base_offset,
          _mode);
        co_await upl->close();
        co_return candidate_creation_error::no_segments_collected;
    }

    if (upl->get_meta().offsets != range) {
        vlog(
          archival_log.debug,
          "{}: offset range was adjusted from {} to {} avoid dirty tail "
          "segment, force upload",
          _manifest.get_ntp(),
          range,
          upl->get_meta().offsets);
        // this means the segment containing range.last was dirty so we bounced
        // back to the previous end. force the upload in this case i guess? or
        // maybe we should
        // just adjust the size-based offset, but then what's the point? maybe
        // we should just say nothing collected in this case? but then we get in
        // a vicious cycle.
        //  - upload size > max --> go to size-based which will by definition
        //    fall below this point
        //  - upload size < min --> go ahead and upload to avoid chasing the
        //    dirty offset. again sort of by definition this should be at
        //    minimum one segment worth of data if it's not empty
        //    - basically need to make sure we don't fall back to size-based in
        //      this case, since that will probably push us back into the dirty
        //      segment
        //  - min <= upload size <= max - GUCCI (don't care)
        force_upload = true;
    }

    auto upload_size = upl->get_size_bytes();

    if (upload_size <= max_size && (force_upload || upload_size >= min_size)) {
        auto meta = co_await std::move(*upl).get_meta();
        // TODO(oren): log to explain why we need to set end_inclusive here
        _end_inclusive = meta.offsets.last;
        co_return upload_candidate_with_locks{
          .candidate = create_candidate(meta),
        };
    } else {
        vlog(
          archival_log.debug,
          "Upload {} size {} out of range [{}..{}]",
          upl->get_meta().offsets,
          upload_size,
          min_size,
          max_size);
        co_await upl->close();
    }

    if (upload_size < min_size) {
        vlog(
          archival_log.debug,
          "Not enough data for upload {} < {}",
          upload_size,
          min_size);
        co_return candidate_creation_error::no_segments_collected;
    }

    // TODO: tweak min_size for timeboxed uploads
    // NOTE(oren): or maybe that's covered by the force_upload check up above?
    size_limited_offset_range sz_range(base_offset, max_size, min_size);

    auto sz_upl_res = co_await segment_upload::make_segment_upload(
      &parent,
      sz_range,
      read_buffer_size,
      ss::default_scheduling_group(),
      deadline);

    if (sz_upl_res.has_error()) {
        if (
          sz_upl_res.error() != archival::error_outcome::not_enough_data
          && sz_upl_res.error() != archival::error_outcome::timed_out) {
            vlog(
              archival_log.error,
              "Can't find upload candidate for {}: {}",
              range,
              sz_upl_res.error().message());
        }
        co_return candidate_creation_error::no_segments_collected;
    }

    auto sz_upl = std::move(sz_upl_res).value();

    auto meta = co_await std::move(*sz_upl).get_meta();
    _end_inclusive = meta.offsets.last;
    co_return upload_candidate_with_locks{
      .candidate = create_candidate(meta),
    };
}

ss::future<segment_collector_stream_result>
segment_collector::make_segment_upload_stream(
  cluster::partition& parent,
  ss::lowres_clock::duration segment_lock_duration,
  ss::gate& gate) {
    auto candidate_res = co_await [this, &parent, &segment_lock_duration]() {
        if (is_v2_mode(_mode)) {
            return make_segment_upload_candidate(parent, segment_lock_duration);
        } else {
            return make_upload_candidate(segment_lock_duration);
        }
    }();

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
          skip.start_offset,
          skip.end_offset,
          skip.reason);
        co_return skip;
    }

    auto& cand_with_locks = std::get<upload_candidate_with_locks>(
      candidate_res);

    auto& cand = cand_with_locks.candidate;

    vlog(
      archival_log.debug,
      "{}: Upload candidate: {}",
      _manifest.get_ntp(),
      cand);

    auto read_buffer_size
      = config::shard_local_cfg().storage_read_buffer_size();
    auto deadline = ss::lowres_clock::now() + segment_lock_duration;

    auto start_offset = cand.starting_offset;
    auto final_offset = [this, &cand]() -> model::offset {
        if (is_reupload_mode(_mode)) {
            return align_end_offset_to_manifest(cand.final_offset)
              .value_or(cand.final_offset);
        }
        return cand.final_offset;
    }();

    if (start_offset > final_offset) {
        vlog(
          archival_log.warn,
          "{}: Invalid offset range for upload: start {} > final {}",
          _manifest.get_ntp(),
          start_offset,
          final_offset);
        co_return candidate_creation_error::no_segments_collected;
    }

    inclusive_offset_range range(start_offset, final_offset);

    ss::gate::holder holder = gate.hold();

    // NOTE: We always allow unstable reads here and accept the result
    // regardless of size because the precise upload bounds have been determined
    // in a previous call to collect_segments. In a future diff, we will be
    // using async_data_uploader as the sole view into the state of the log, so
    // unstable reads will not always be appropriate.
    auto upl = co_await segment_upload::make_segment_upload(
      &parent,
      range,
      read_buffer_size,
      ss::default_scheduling_group(),
      deadline,
      true /* allow_unstable_reads */);

    if (upl.has_error()) {
        // NOTE: under load, it's not uncommon for read lock acquisition to
        // time out, e.g. due to a race with prefix truncation.
        if (
          upl.error() != archival::error_outcome::not_enough_data
          && upl.error() != archival::error_outcome::timed_out) {
            vlog(
              archival_log.error,
              "Can't find upload candidate for {}: {}",
              range,
              upl.error().message());
        }
        co_return candidate_creation_error::no_segments_collected;
    }

    auto seg_upload = std::move(upl).value();
    auto meta = seg_upload->get_meta();

    // Now that we know the final size of the reupload, perform
    // a final sanity check to ensure that the size of the new segment
    // is smaller than that of the replaced one. Skip the upload if that's
    // not the case.
    // TODO(oren): I guess if we combined segments then we're totally happy with
    // this
    if (auto to_replace = _manifest.find(meta.offsets.base);
        to_replace != _manifest.end()
        && to_replace->committed_offset == meta.offsets.last) {
        if (to_replace->size_bytes <= meta.size_bytes) {
            vlog(
              archival_log.debug,
              "Skipping compacted reupload as its size has not "
              "decreased as a result of self - compaction ");
            co_await seg_upload->close();
            co_return skip_offset_range{
              .start_offset = meta.offsets.base,
              .end_offset = meta.offsets.last,
              .reason = candidate_creation_error::upload_size_unchanged};
        }
    }

    vlog(
      archival_log.debug,
      "{}: {{b: {} e: {}}} Got upload:  offsets: {} sz: {} compact?: {}",
      _manifest.get_ntp(),
      start_offset,
      final_offset,
      meta.offsets,
      seg_upload->get_size_bytes(),
      seg_upload->get_meta().is_compacted
        && seg_upload->get_meta().compaction_complete);

    segment_collector_stream stream;
    stream.start_offset = meta.offsets.base;
    stream.end_offset = meta.offsets.last;
    stream.min_timestamp = meta.base_timestamp;
    stream.max_timestamp = meta.max_timestamp;
    stream.size = meta.size_bytes;
    stream.is_compacted = meta.is_compacted && meta.compaction_complete;
    stream.term = parent.get_term(meta.offsets.base);
    auto strm = co_await std::move(*seg_upload).detach_stream();
    stream.create_input_stream =
      [strm = std::move(strm),
       holder = std::move(holder)]() mutable -> ss::input_stream<char> {
        // NOTE: should only be called w/ a gate held
        holder.release();
        return std::move(strm);
    };
    co_return stream;
}

size_t segment_collector::collected_size() const { return _collected_size; }

} // namespace archival
