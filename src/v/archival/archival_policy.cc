/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archival_policy.h"

#include "archival/logger.h"
#include "archival/segment_reupload.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/offset_to_filepos.h"
#include "storage/offset_translator_state.h"
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"
#include "vlog.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <utility>

constexpr size_t compacted_segment_size_multiplier{3};

namespace archival {

using namespace std::chrono_literals;

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
  ss::shared_ptr<storage::log> log,
  const storage::offset_translator_state& ot_state) {
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
    if (it == set.end() || (*it)->finished_self_compaction()) {
        // Skip forward if we hit a gap or compacted segment
        for (auto i = set.begin(); i != set.end(); i++) {
            const auto& sg = *i;
            if (start_offset < sg->offsets().base_offset) {
                // Move last offset forward
                it = i;
                start_offset = sg->offsets().base_offset;
                break;
            }
        }
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
    // Invariant: it != set.end()
    bool closed = !(*it)->has_appender();
    bool force_upload = upload_deadline_reached();
    if (!closed && !force_upload) {
        std::string_view reason = _upload_limit.has_value()
                                    ? "upload deadline not reached"
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

    if (!closed) {
        auto kafka_start_offset = ot_state.from_log_offset(start_offset);
        auto kafka_lso = ot_state.from_log_offset(
          model::next_offset(adjusted_lso));
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

    auto dirty_offset = (*it)->offsets().dirty_offset;
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
    upl->starting_offset = segment->offsets().base_offset;
    upl->file_offset = 0;
    upl->content_length = fsize;
    upl->final_offset = segment->offsets().dirty_offset;
    upl->final_file_offset = fsize;
    upl->base_timestamp = segment->index().base_timestamp();
    upl->max_timestamp = segment->index().max_timestamp();
    if (!end_inclusive && segment->offsets().base_offset == begin_inclusive) {
        // Fast path, the upload is started at the begining of the segment
        // and not truncted at the end.
        vlog(
          archival_log.debug,
          "Full segment upload {}, file size: {}",
          upl,
          fsize);
        co_return std::nullopt;
    }
    if (begin_inclusive != segment->offsets().base_offset) {
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
static ss::future<upload_candidate_with_locks> create_upload_candidate(
  model::offset begin_inclusive,
  std::optional<model::offset> end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  const storage::ntp_config* ntp_conf,
  ss::io_priority_class io_priority,
  ss::lowres_clock::duration segment_lock_duration) {
    auto term = segment->offsets().term;
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
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }
    if (result->starting_offset != segment->offsets().base_offset) {
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

ss::future<upload_candidate_with_locks> archival_policy::get_next_candidate(
  model::offset begin_inclusive,
  model::offset end_exclusive,
  ss::shared_ptr<storage::log> log,
  const storage::offset_translator_state& ot_state,
  ss::lowres_clock::duration segment_lock_duration) {
    // NOTE: end_exclusive (which is initialized with LSO) points to the first
    // unstable recordbatch we need to look at the previous batch if needed.
    auto adjusted_lso = end_exclusive - model::offset(1);
    auto [segment, ntp_conf, forced] = find_segment(
      begin_inclusive, adjusted_lso, std::move(log), ot_state);
    if (segment.get() == nullptr || ntp_conf == nullptr) {
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
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
    if (upload.candidate.content_length == 0) {
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }
    co_return upload;
}

ss::future<upload_candidate_with_locks>
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
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }
    segment_collector compacted_segment_collector{
      begin_inclusive,
      manifest,
      *log,
      config::shard_local_cfg().compacted_log_segment_size
        * compacted_segment_size_multiplier};

    compacted_segment_collector.collect_segments();
    if (!compacted_segment_collector.should_replace_manifest_segment()) {
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }

    co_return co_await compacted_segment_collector.make_upload_candidate(
      _io_priority, segment_lock_duration);
}

} // namespace archival
