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
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/offset_to_filepos.h"
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"

#include <seastar/core/io_priority_class.hh>
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
  model::ntp ntp,
  std::optional<segment_time_limit> limit,
  ss::io_priority_class io_priority)
  : _ntp(std::move(ntp))
  , _upload_limit(limit)
  , _io_priority(io_priority) {}

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

archival_policy::lookup_result archival_policy::find_segment(
  model::offset start_offset,
  model::offset adjusted_lso,
  std::optional<model::offset> flush_offset,
  ss::shared_ptr<storage::log> log) {
    vlog(
      archival_log.debug,
      "Upload policy for {} invoked, start offset: {}",
      _ntp,
      start_offset);
    if (log->segment_count() == 0) {
        vlog(
          archival_log.debug,
          "Upload policy for {}: can't find candidate, no segments",
          _ntp);
        return {};
    }

    const auto& set = log->segments();
    const auto& ntp_conf = log->config();
    auto it = set.lower_bound(start_offset);
    if (it == set.end() && start_offset < log->offsets().committed_offset) {
        // The 'start_offset' is in the gap. Normally this shouldn't happen.
        vlog(
          archival_log.warn,
          "Upload policy for {}: can't find segment with base_offset={}",
          _ntp,
          start_offset);
        it = std::find_if(
          set.begin(),
          set.end(),
          [start_offset](const ss::lw_shared_ptr<storage::segment>& s) {
              return s->offsets().get_base_offset() >= start_offset;
          });
    }
    if (it == set.end()) {
        vlog(
          archival_log.debug,
          "Upload policy for {}: can't find candidate, all segment offsets are "
          "less than start_offset: {}",
          _ntp,
          start_offset);
        return {};
    }
    if (start_offset > (*it)->offsets().get_committed_offset()) {
        vlog(
          archival_log.debug,
          "Upload policy for {}: can't find candidate, found segment's "
          "committed offset is less than start_offset {}: {}",
          _ntp,
          start_offset,
          *it);
        return {};
    }
    // Invariant: it != set.end()
    bool closed = !(*it)->has_appender();
    bool below_flush_offset = flush_offset.has_value()
                              && (*it)->offsets().get_base_offset()
                                   <= flush_offset.value();
    bool force_upload = below_flush_offset || upload_deadline_reached();
    if (!closed && !force_upload) {
        std::string_view reason
          = closed ? "upload is not forced by flush, or deadline not reached"
                   : "candidate is not closed";
        // Fast path, next upload candidate is not yet ready. We may want to
        // optimize this case because it's expected to happen pretty often. This
        // can be done by saving weak_ptr to the segment inside the policy
        // object. The segment must be changed to derive from
        // ss::weakly_referencable.
        vlog(
          archival_log.debug,
          "Upload policy for {}: can't find candidate, {}",
          _ntp,
          reason);
        return {};
    }

    if (!closed && !below_flush_offset) {
        auto kafka_start_offset = log->from_log_offset(start_offset);
        auto kafka_lso = log->from_log_offset(model::next_offset(adjusted_lso));
        if (kafka_start_offset >= kafka_lso) {
            // If timeboxed uploads are enabled and there is no producer
            // activity, we can get into a nasty loop where we upload a segment,
            // add an archival metadata batch, upload a segment containing that
            // batch, add another archival metadata batch, etc. This leads to
            // lots of small segments that don't contain data being uploaded. To
            // avoid it, we check that kafka (translated) offset increases.
            vlog(
              archival_log.debug,
              "Upload policy for {}: can't find candidate, only non-data "
              "batches to upload (kafka start_offset: {}, kafka "
              "last_stable_offset: {})",
              _ntp,
              kafka_start_offset,
              kafka_lso);
            return {};
        }
    }

    auto dirty_offset = (*it)->offsets().get_dirty_offset();
    if (dirty_offset > adjusted_lso && !force_upload) {
        vlog(
          archival_log.debug,
          "Upload policy for {}: can't find candidate, candidate dirty offset "
          "{} is above last_stable_offset {}",
          _ntp,
          dirty_offset,
          adjusted_lso);
        return {};
    }
    if (_upload_limit) {
        _upload_deadline = ss::lowres_clock::now() + _upload_limit.value()();
    }
    return {.segment = *it, .ntp_conf = &ntp_conf, .forced = force_upload};
}

/// This function computes offsets for the upload (inc. file offets)
/// If the full segment is uploaded the segment is not scanned.
/// If the upload is partial, the partial scan will be performed if
/// the segment has the index and full scan otherwise.
static ss::future<std::optional<std::error_code>> get_file_range(
  model::offset begin_inclusive,
  std::optional<model::offset> end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  ss::lw_shared_ptr<upload_candidate> upl,
  ss::io_priority_class io_priority) {
    size_t fsize = segment->reader().file_size();
    // These are default values for full segment upload.
    // We start with these values and refine them further
    // down the code path.
    upl->starting_offset = segment->offsets().get_base_offset();
    upl->file_offset = 0;
    upl->content_length = fsize;
    upl->final_offset = segment->offsets().get_dirty_offset();
    upl->final_file_offset = fsize;
    upl->base_timestamp = segment->index().base_timestamp();
    upl->max_timestamp = segment->index().max_timestamp();
    if (
      !end_inclusive
      && segment->offsets().get_base_offset() == begin_inclusive) {
        // Fast path, the upload is started at the begining of the segment
        // and not truncted at the end.
        vlog(
          archival_log.debug,
          "Full segment upload {}, file size: {}",
          upl,
          fsize);
        co_return std::nullopt;
    }
    if (begin_inclusive != segment->offsets().get_base_offset()) {
        auto seek_result = co_await storage::convert_begin_offset_to_file_pos(
          begin_inclusive, segment, upl->base_timestamp, io_priority);
        if (seek_result.has_error()) {
            co_return seek_result.error();
        }
        auto seek = seek_result.value();
        upl->starting_offset = seek.offset;
        upl->file_offset = seek.bytes;
        upl->base_timestamp = seek.ts;
    }
    if (end_inclusive) {
        auto seek_result = co_await storage::convert_end_offset_to_file_pos(
          end_inclusive.value(),
          segment,
          upl->max_timestamp,
          io_priority,
          // Even if the offset is not found in the segment, allow upload
          // candidate creation for non-compacted upload. The entire segment
          // will be used to create the upload candidate, ensuring progress.
          storage::should_fail_on_missing_offset::no);
        if (seek_result.has_error()) {
            co_return seek_result.error();
        }
        auto seek = seek_result.value();
        upl->final_offset = seek.offset;
        upl->final_file_offset = seek.bytes;
        upl->max_timestamp = seek.ts;
    }
    // Recompute content_length based on file offsets
    vassert(
      upl->file_offset <= upl->final_file_offset,
      "Invalid upload candidate {}",
      upl);
    upl->content_length = upl->final_file_offset - upl->file_offset;
    if (upl->content_length > segment->reader().file_size()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Incorrect content length in {}, file size {}",
          upl,
          segment->reader().file_size()));
    }
    vlog(
      archival_log.debug,
      "Partial segment upload {}, original file size: {}",
      upl,
      fsize);
    co_return std::nullopt;
}

/// \brief Initializes upload_candidate structure taking into account
///        possible segment overlaps at 'off'
///
/// \param begin_inclusive is a last_offset from manifest
/// \param end_inclusive is a last offset to upload
/// \param segment is a segment that has this offset
/// \param ntp_conf is a ntp_config of the partition
///
/// \note Normally, the segments on a single node doesn't overlap,
///       but when leadership changes the new node will have to deal
///       with the situaiton when 'last_offset' doesn't match base
///       offset of any segment. In this situation we need to find
///       the segment that contains the 'last_offset' and find the
///       exact location of the 'last_offset' inside the segment (
///       or nearset offset, because index is sampled). Then we need
///       to compute file offset of the upload, content length and
///       new base offset (and new segment name for S3).
///       Example: last_offset is 1000, and we have segment with the
///       name '900-1-v1.log'. We should find offset 1000 inside it
///       and upload starting from it. The resulting file should have
///       a name '1000-1-v1.log'. If we were only able to find offset
///       990 instead of 1000, we will upload starting from it and
///       the name will be '990-1-v1.log'.
static ss::future<candidate_creation_result> create_upload_candidate(
  model::offset begin_inclusive,
  std::optional<model::offset> end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  const storage::ntp_config* ntp_conf,
  ss::io_priority_class io_priority,
  ss::lowres_clock::duration segment_lock_duration) {
    auto term = segment->offsets().get_term();
    auto version = storage::record_version_type::v1;
    auto meta = storage::segment_path::parse_segment_filename(
      segment->filename());
    if (meta) {
        version = meta->version;
    }

    auto deadline = std::chrono::steady_clock::now() + segment_lock_duration;
    std::vector<ss::rwlock::holder> locks;
    vlog(
      archival_log.trace,
      "Acquiring read lock for segment {}",
      segment->filename());
    locks.emplace_back(co_await segment->read_lock(deadline));
    vlog(
      archival_log.trace,
      "Read lock acquired for segment {}",
      segment->filename());
    auto result = ss::make_lw_shared<upload_candidate>(
      {.term = term, .sources = {segment}});

    auto file_range_result = co_await get_file_range(
      begin_inclusive, end_inclusive, segment, result, io_priority);
    if (file_range_result) {
        vlog(
          archival_log.error,
          "Upload candidate not created, failed to get file range: {}",
          file_range_result.value().message());
        co_return candidate_creation_error::failed_to_get_file_range;
    }
    if (result->starting_offset != segment->offsets().get_base_offset()) {
        // We need to generate new name for the segment
        auto path = storage::segment_path::make_segment_path(
          *ntp_conf, result->starting_offset, term, version);
        result->exposed_name = segment_name(path.filename().string());
        vlog(
          archival_log.debug,
          "Using adjusted segment name: {}",
          result->exposed_name);
    } else {
        auto orig_path = std::filesystem::path(segment->filename());
        result->exposed_name = segment_name(orig_path.filename().string());
        vlog(
          archival_log.debug,
          "Using original segment name: {}",
          result->exposed_name);
    }

    co_return upload_candidate_with_locks{*result, std::move(locks)};
}

ss::future<candidate_creation_result> archival_policy::get_next_candidate(
  model::offset begin_inclusive,
  model::offset end_exclusive,
  std::optional<model::offset> flush_offset,
  ss::shared_ptr<storage::log> log,
  ss::lowres_clock::duration segment_lock_duration) {
    // NOTE: end_exclusive (which is initialized with LSO) points to the first
    // unstable recordbatch we need to look at the previous batch if needed.
    auto adjusted_lso = end_exclusive - model::offset(1);
    auto [segment, ntp_conf, forced] = find_segment(
      begin_inclusive, adjusted_lso, flush_offset, std::move(log));
    if (segment.get() == nullptr) {
        co_return candidate_creation_error::no_segment_for_begin_offset;
    }

    if (ntp_conf == nullptr) {
        co_return candidate_creation_error::missing_ntp_config;
    }
    // We need to adjust LSO since it points to the first
    // recordbatch with uncommitted transactions data
    auto last = forced ? std::make_optional(adjusted_lso) : std::nullopt;
    vlog(
      archival_log.debug,
      "Upload policy for {}, creating upload candidate: start_offset {}, "
      "segment offsets {}, adjusted LSO {}",
      _ntp,
      begin_inclusive,
      segment->offsets(),
      adjusted_lso);
    auto upload = co_await create_upload_candidate(
      begin_inclusive,
      last,
      segment,
      ntp_conf,
      _io_priority,
      segment_lock_duration);
    if (const auto* u = std::get_if<upload_candidate_with_locks>(&upload); u) {
        if (u->candidate.content_length == 0) {
            co_return candidate_creation_error::zero_content_length;
        }
    }
    co_return upload;
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
      _io_priority, segment_lock_duration);
}

} // namespace archival
