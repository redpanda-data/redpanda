/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/archival/probe.h"
#include "cluster/archival/types.h"
#include "model/fundamental.h"
#include "storage/fwd.h"
#include "storage/ntp_config.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/rwlock.hh>

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

/// Archival policy is responsible for extracting segments from
/// log_manager in right order.
///
/// \note It doesn't store a reference to log_manager or any segments
/// but uses ntp as a key to extract the data when needed.
class archival_policy {
public:
    explicit archival_policy(
      model::ntp ntp,
      std::optional<segment_time_limit> limit = std::nullopt,
      ss::io_priority_class io_priority = ss::default_priority_class());

    /// \brief regurn next upload candidate
    ///
    /// \param begin_inclusive is an inclusive begining of the range
    /// \param end_exclusive is an exclusive end of the range
    /// \param lm is a log manager
    /// \return initializd struct on success, empty struct on failure
    ss::future<candidate_creation_result> get_next_candidate(
      model::offset begin_inclusive,
      model::offset end_exclusive,
      std::optional<model::offset> flush_offset,
      ss::shared_ptr<storage::log>,
      ss::lowres_clock::duration segment_lock_duration);

    ss::future<candidate_creation_result> get_next_compacted_segment(
      model::offset begin_inclusive,
      ss::shared_ptr<storage::log> log,
      const cloud_storage::partition_manifest& manifest,
      ss::lowres_clock::duration segment_lock_duration);

    static bool eligible_for_compacted_reupload(const storage::segment&);

private:
    /// Check if the upload have to be forced due to timeout
    ///
    /// If the upload is idle longer than expected the next call to
    /// `get_next_candidate` will return partial result which will
    /// result in partial upload.
    bool upload_deadline_reached();

    struct lookup_result {
        ss::lw_shared_ptr<storage::segment> segment;
        const storage::ntp_config* ntp_conf;
        bool forced;
    };

    lookup_result find_segment(
      model::offset last_offset,
      model::offset adjusted_lso,
      std::optional<model::offset> flush_offset,
      ss::shared_ptr<storage::log>);

    model::ntp _ntp;
    std::optional<segment_time_limit> _upload_limit;
    std::optional<ss::lowres_clock::time_point> _upload_deadline;
    ss::io_priority_class _io_priority;
};

} // namespace archival
