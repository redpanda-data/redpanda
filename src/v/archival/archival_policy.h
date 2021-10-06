/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/probe.h"
#include "archival/types.h"
#include "model/fundamental.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment_set.h"

namespace archival {

struct upload_candidate {
    ss::lw_shared_ptr<storage::segment> source;
    segment_name exposed_name;
    model::offset starting_offset;
    size_t file_offset;
    size_t content_length;
    model::offset final_offset;
    size_t final_file_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
};

std::ostream& operator<<(std::ostream& s, const upload_candidate& c);

/// Archival policy is responsible for extracting segments from
/// log_manager in right order.
///
/// \note It doesn't store a reference to log_manager or any segments
/// but uses ntp as a key to extract the data when needed.
class archival_policy {
public:
    explicit archival_policy(
      model::ntp ntp,
      service_probe& svc_probe,
      ntp_level_probe& ntp_probe,
      std::optional<segment_time_limit> limit = std::nullopt);

    /// \brief regurn next upload candidate
    ///
    /// \param begin_inclusive is an inclusive begining of the range
    /// \param end_exclusive is an exclusive end of the range
    /// \param lm is a log manager
    /// \return initializd struct on success, empty struct on failure
    ss::future<upload_candidate> get_next_candidate(
      model::offset begin_inclusive,
      model::offset end_exclusive,
      storage::log_manager& lm);

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
      storage::log_manager& lm);

    model::ntp _ntp;
    service_probe& _svc_probe;
    ntp_level_probe& _ntp_probe;
    std::optional<segment_time_limit> _upload_limit;
    std::optional<ss::lowres_clock::time_point> _upload_deadline;
};

} // namespace archival
