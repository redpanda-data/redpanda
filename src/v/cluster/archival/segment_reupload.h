/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/types.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "storage/fwd.h"
#include "storage/log.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

namespace cloud_storage {
class partition_manifest;
} // namespace cloud_storage

namespace storage {
class disk_log_impl;
class ntp_config;
class segment;
} // namespace storage

namespace archival {

enum class candidate_creation_error {
    no_segments_collected,
    begin_offset_seek_error,
    end_offset_seek_error,
    offset_inside_batch,
    upload_size_unchanged,
    cannot_replace_manifest_entry,
    no_segment_for_begin_offset,
    missing_ntp_config,
    failed_to_get_file_range,
    zero_content_length,
    concurrency_error,
};

std::ostream& operator<<(std::ostream&, candidate_creation_error);

ss::log_level log_level_for_error(const candidate_creation_error& error);

struct upload_candidate {
    segment_name exposed_name;
    model::offset starting_offset;
    size_t file_offset;
    size_t content_length;
    model::offset final_offset;
    size_t final_file_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    model::term_id term;
    std::vector<ss::lw_shared_ptr<storage::segment>> sources;
    std::vector<cloud_storage::remote_segment_path> remote_sources;

    friend std::ostream& operator<<(std::ostream& s, const upload_candidate& c);
};

struct upload_candidate_with_locks {
    upload_candidate candidate;
    std::vector<ss::rwlock::holder> read_locks;
};

/// Wraps an error with an offset range, so that no
/// further upload candidates are created from this offset range.
struct skip_offset_range {
    model::offset begin_offset;
    model::offset end_offset;
    candidate_creation_error reason;

    friend std::ostream& operator<<(std::ostream&, const skip_offset_range&);
};

using candidate_creation_result = std::variant<
  std::monostate,
  upload_candidate_with_locks,
  skip_offset_range,
  candidate_creation_error>;

enum class segment_collector_mode {
    // collect segments for reupload
    compacted_reupload,
    // collect segments for reupload
    non_compacted_reupload,
    // collect segments for the first time upload
    // may be compacted or non-compacted
    new_upload,
};

struct segment_collector_stream {
    // The offset range for the segments that are being uploaded.
    model::offset start_offset, end_offset;
    size_t size;
    // The time range of the segments that are being uploaded.
    model::timestamp min_timestamp, max_timestamp;

    bool is_compacted{false};

    // Create the input_stream for the segment upload.
    // The generator function here is stateful and holds the segments and the
    // locks. The function can only be called once.
    // The object should be kept alive until the stream is alive.
    ss::noncopyable_function<ss::input_stream<char>()> create_input_stream;

    model::term_id term;

    ss::future<> close() { co_await create_input_stream().close(); }

    friend std::ostream&
    operator<<(std::ostream& s, const segment_collector_stream&);
};

using segment_collector_stream_result = std::variant<
  std::monostate,
  segment_collector_stream,
  skip_offset_range,
  candidate_creation_error>;

bool eligible_for_compacted_reupload(const storage::segment&);

class segment_collector {
public:
    using segment_seq = std::vector<ss::lw_shared_ptr<storage::segment>>;
    using generation_seq = std::vector<uint64_t>;
    using sizes_seq = std::vector<uint64_t>;

    segment_collector(
      segment_collector_mode mode,
      model::offset begin_inclusive,
      const cloud_storage::partition_manifest& manifest,
      const storage::log& log,
      size_t max_uploaded_segment_size,
      std::optional<model::offset> end_inclusive = std::nullopt);

    /// C-tor
    ///
    /// \param begin_inclusive is a first offset in range
    /// \param manifest is a partition manifest
    /// \param log is a partition log
    /// \param max_uploaded_segment_size is a size limit for the offset range
    /// \param end_inclusive is a target for the end offset (if not set the
    ///        collector will try to match the size only)
    /// \param end_exclusive last stable offset (unadjusted) of the target
    ///        partition. in new upload mode, we won't upload a segment if
    ///        it's dirty offset exceeds this value
    /// \param flush_offset offset specified by a flush operation upstream,
    ///        has no effect in reupload mode
    segment_collector(
      segment_collector_mode mode,
      model::offset begin_inclusive,
      const cloud_storage::partition_manifest& manifest,
      const storage::log& log,
      size_t max_uploaded_segment_size,
      std::optional<model::offset> end_inclusive,
      std::optional<model::offset> end_exclusive,
      std::optional<model::offset> flush_offset);

    /// Collect segments
    ///
    /// \param mode defines what segments should be collected
    ///        compacted or normal.
    void collect_segments();

    segment_seq segments();

    /// Once segments are collected, this query determines if the collected
    /// segments should replace at least one segment in manifest.
    bool should_replace_manifest_segment() const;

    /// Segments are found and can be uploaded to the cloud.
    bool segment_ready_for_upload() const;

    /// The starting point for the collection, this may not coincide with the
    /// start of the first collected segment. It should be aligned
    /// with the manifest segment boundary.
    model::offset begin_inclusive() const;

    /// The ending point for the collection, aligned with manifest segment
    /// boundary.
    model::offset end_inclusive() const;

    const storage::ntp_config* ntp_cfg() const;

    cloud_storage::segment_name adjust_segment_name() const;

    /// Creates upload candidate by computing file offsets and timestamps from
    /// the collected segments.
    ss::future<candidate_creation_result>
    make_upload_candidate(ss::lowres_clock::duration segment_lock_duration);

    size_t collected_size() const;

    // Create a stream for the upload candidate.
    ss::future<segment_collector_stream_result> make_upload_candidate_stream(
      ss::lowres_clock::duration segment_lock_duration);

    ss::future<segment_collector_stream_result> make_segment_upload_stream(
      cluster::partition& parent,
      ss::lowres_clock::duration segment_lock_duration,
      ss::gate& gate);

private:
    struct lookup_result {
        segment_seq::value_type segment;
        const storage::ntp_config* ntp_conf;
    };

    ss::lw_shared_ptr<storage::segment>
    lower_bound(model::offset, segment_collector_mode mode) const;

    /// Collects segments until the end of the manifest, or until the
    /// end of compacted segments in log.
    void do_collect();

    lookup_result
    find_next_segment(model::offset start_offset, segment_collector_mode mode);

    /// Makes sure that the begin offset of the collection is aligned to the
    /// manifest segment boundary. If the begin offset is inside a manifest
    /// segment, we advance the offset enough to the beginning of the next
    /// manifest segment, so that when we re-upload segments there is no
    /// overlap.
    void align_begin_offset_to_manifest();

    /// Makes sure that the end offset of the collection is aligned to the
    /// manifest segment boundary. If the end offset is inside a manifest
    /// segment, we decrement the offset enough to the end of the previous
    /// manifest segment, so that when we re-upload segments there is no
    /// overlap.
    std::optional<model::offset>
    align_end_offset_to_manifest(model::offset compacted_segment_end);

    /// Finds the offset which the collection needs to progress upto in order to
    /// replace at least one manifest segment. The collection is valid if it
    /// reaches the replacement boundary.
    model::offset find_replacement_boundary(segment_collector_mode mode) const;

    // TODO(oren): maybe consolidate into some kind of struct that can be wired
    // through the APIs to make the collector less stateful
    model::offset _begin_inclusive;
    model::offset _end_inclusive{};

    const cloud_storage::partition_manifest& _manifest;
    const storage::log& _log;
    const storage::ntp_config* _ntp_cfg{};
    segment_seq _segments;
    generation_seq _generations;
    sizes_seq _sizes;
    bool _can_replace_manifest_segment{false};

    size_t _max_uploaded_segment_size;
    std::optional<model::offset> _target_end_inclusive;
    size_t _collected_size;
    std::optional<model::offset> _end_exclusive;
    std::optional<model::offset> _flush_offset;
    segment_collector_mode _mode;
};

} // namespace archival
