/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/archival_policy.h"

#include "base/vlog.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/segment_reupload.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/offset_to_filepos.h"
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <utility>

constexpr size_t compacted_segment_size_multiplier{3};

namespace archival {

using namespace std::chrono_literals;

bool archival_policy::eligible_for_compacted_reupload(
  const storage::segment& s) {
    if (config::shard_local_cfg().log_compaction_use_sliding_window) {
        return s.finished_windowed_compaction();
    }
    return s.finished_self_compaction();
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

archival_policy::archival_policy(
  model::ntp ntp, std::optional<segment_time_limit> limit)
  : _ntp(std::move(ntp))
  , _upload_limit(limit) {}

bool archival_policy::upload_deadline_reached() {
    if (!_upload_limit.has_value()) {
        return false;
    } else if (_upload_limit.value() == 0s) {
        // This code path is only used to trigger partial upload
        // in test envronment.
        return true;
    }
    auto now = ss::lowres_clock::now();

    if (!_upload_deadline.has_value()) {
        _upload_deadline = now + (*_upload_limit)();
    }
    return _upload_deadline < now;
}

ss::future<candidate_creation_result> archival_policy::get_next_segment(
  model::offset begin_inclusive,
  model::offset end_exclusive,
  std::optional<model::offset> flush_offset,
  ss::shared_ptr<storage::log> log,
  const cloud_storage::partition_manifest& manifest,
  ss::lowres_clock::duration segment_lock_duration) {
    std::optional<model::offset> end_inclusive;
    bool force_upload = flush_offset.has_value() || upload_deadline_reached();
    if (force_upload) {
        end_inclusive = flush_offset.value_or(
          model::prev_offset(end_exclusive));
    }
    vlog(
      archival_log.debug,
      "get_next_segment {}, begin_inclusive: {}, end_exclusive: {}, "
      "end_inclusive: {}, force_upload: {}",
      _ntp,
      begin_inclusive,
      end_exclusive,
      end_inclusive,
      force_upload);

    segment_collector segment_collector{
      begin_inclusive,
      manifest,
      *log,
      config::shard_local_cfg().cloud_storage_segment_size_target().value_or(
        config::shard_local_cfg().log_segment_size),
      end_inclusive,
      end_exclusive,
      flush_offset};

    segment_collector.collect_segments(segment_collector_mode::new_upload);
    if (!segment_collector.segment_ready_for_upload()) {
        co_return candidate_creation_error::no_segments_collected;
    }

    if (_upload_limit) {
        _upload_deadline = ss::lowres_clock::now() + _upload_limit.value()();
    }
    co_return co_await segment_collector.make_upload_candidate(
      segment_lock_duration);
}

ss::future<candidate_creation_result>
archival_policy::get_next_compacted_segment(
  model::offset begin_inclusive,
  ss::shared_ptr<storage::log> log,
  const cloud_storage::partition_manifest& manifest,
  ss::lowres_clock::duration segment_lock_duration) {
    if (log->segment_count() == 0) {
        vlog(
          archival_log.warn,
          "Upload policy find next compacted segment: no segments ntp: {}",
          _ntp);
        co_return candidate_creation_error::no_segments_collected;
    }
    segment_collector compacted_segment_collector{
      begin_inclusive,
      manifest,
      *log,
      config::shard_local_cfg().compacted_log_segment_size
        * compacted_segment_size_multiplier};

    compacted_segment_collector.collect_segments();
    if (!compacted_segment_collector.should_replace_manifest_segment()) {
        co_return candidate_creation_error::cannot_replace_manifest_entry;
    }

    co_return co_await compacted_segment_collector.make_upload_candidate(
      segment_lock_duration);
}

} // namespace archival
