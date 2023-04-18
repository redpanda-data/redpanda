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

#include "archival/probe.h"
#include "archival/types.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "storage/fwd.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment_set.h"

#include <seastar/core/io_priority_class.hh>

namespace archival {

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
    /// \param end_inclusive is an inclusive end of the range
    /// \param lm is a log manager
    /// \return initializd struct on success, empty struct on failure
    ss::future<upload_candidate_with_locks> get_next_candidate(
      model::offset begin_inclusive,
      model::offset end_inclusive,
      storage::log,
      const storage::offset_translator_state&,
      ss::lowres_clock::duration segment_lock_duration);

    ss::future<upload_candidate_with_locks> get_next_compacted_segment(
      model::offset begin_inclusive,
      storage::log log,
      const cloud_storage::partition_manifest& manifest,
      ss::lowres_clock::duration segment_lock_duration);

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
      model::offset start_offset,
      model::offset last_offset_inclusive,
      storage::log,
      const storage::offset_translator_state&);

    model::ntp _ntp;
    std::optional<segment_time_limit> _upload_limit;
    std::optional<ss::lowres_clock::time_point> _upload_deadline;
    ss::io_priority_class _io_priority;
};

} // namespace archival
