// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"

#include <vector>

namespace archival {

/// Represents a series of adjacent segments
/// The object is used to compute a possible reupload
/// candidate. The series of segment is supposed to be
/// merged and reuploaded. The object produces metadata
/// for the reuploaded segment.
struct adjacent_segment_run {
    explicit adjacent_segment_run(model::ntp ntp)
      : ntp(std::move(ntp)) {}

    model::ntp ntp;
    cloud_storage::segment_meta meta{};
    size_t num_segments{0};
    std::vector<cloud_storage::remote_segment_path> segments;

    /// Try to add segment to the run
    ///
    /// The subsequent calls are successful until the total size
    /// of the run is below the threshold. The object keeps track
    /// of all segment names.
    ///
    /// \return true if the run is assembled, false if more segments can be
    ///         added to the run
    bool maybe_add_segment(
      const cloud_storage::partition_manifest& manifest,
      const cloud_storage::segment_meta& s,
      size_t max_size,
      const cloud_storage::remote_path_provider& path_provider);
};

std::ostream& operator<<(std::ostream& o, const adjacent_segment_run& run);

} // namespace archival
