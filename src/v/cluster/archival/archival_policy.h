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
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/types.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "storage/fwd.h"

#include <seastar/core/rwlock.hh>

namespace archival {

bool use_v2_segment_collector();

/// Archival policy is responsible for extracting segments from
/// log_manager in right order.
///
/// \note It doesn't store a reference to log_manager or any segments
/// but uses ntp as a key to extract the data when needed.
class archival_policy {
public:
    explicit archival_policy(
      model::ntp ntp,
      cluster::partition& parent,
      ss::gate& gate,
      std::optional<segment_time_limit> limit = std::nullopt);

    ss::future<segment_collector_stream_result> get_next_compacted_segment(
      model::offset begin_inclusive,
      ss::shared_ptr<storage::log> log,
      const cloud_storage::partition_manifest& manifest,
      ss::lowres_clock::duration segment_lock_duration);

    ss::future<segment_collector_stream_result> get_next_segment(
      model::offset begin_inclusive,
      model::offset end_exclusive,
      std::optional<model::offset> flush_offset,
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

    model::ntp _ntp;
    std::optional<segment_time_limit> _upload_limit;
    std::optional<ss::lowres_clock::time_point> _upload_deadline;
    cluster::partition* _parent;
    ss::gate* _gate;
};

} // namespace archival
