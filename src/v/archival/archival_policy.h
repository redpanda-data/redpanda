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
      model::ntp ntp, service_probe& svc_probe, ntp_level_probe& ntp_probe);

    /// \brief regurn next upload candidate
    ///
    /// \param last_offset is a last uploaded offset
    /// \param high_watermark is current high_watermark offset for the partition
    /// \param lm is a log manager
    /// \return initializd struct on success, empty struct on failure
    /// \note returned upload candidate can have offset which is smaller than
    ///       last_offset because index is sparse and don't have all possible
    ///       offsets. If index is not materialized we will upload log starting
    ///       from the begining.
    upload_candidate get_next_candidate(
      model::offset last_offset,
      model::offset high_watermark,
      storage::log_manager& lm);

private:
    struct lookup_result {
        ss::lw_shared_ptr<storage::segment> segment;
        const storage::ntp_config* ntp_conf;
    };

    lookup_result find_segment(
      model::offset last_offset,
      model::offset high_watermark,
      storage::log_manager& lm);

    model::ntp _ntp;
    service_probe& _svc_probe;
    ntp_level_probe& _ntp_probe;
};

} // namespace archival