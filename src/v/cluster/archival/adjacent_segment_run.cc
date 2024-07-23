// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cluster/archival/adjacent_segment_run.h"

#include "base/vlog.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "cluster/archival/logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <vector>

namespace archival {

bool adjacent_segment_run::maybe_add_segment(
  const cloud_storage::partition_manifest& manifest,
  const cloud_storage::segment_meta& s,
  size_t max_size,
  const cloud_storage::remote_path_provider& path_provider) {
    vlog(
      archival_log.debug,
      "{} Segments collected, looking at segment meta: {}, current run meta: "
      "{}",
      num_segments,
      s,
      meta);
    auto remote_path = manifest.generate_segment_path(s, path_provider);
    if (num_segments == 1 && meta.size_bytes + s.size_bytes > max_size) {
        // Corner case, we hit a segment which is smaller than the max_size
        // but it's larger than max_size when combined with its neighbor. In
        // this case we need to skip previous the segment.
        num_segments = 0;
        segments.clear();
        meta = {};
    }
    if (num_segments == 0) {
        // Find the begining of the small segment
        // run.
        if (s.size_bytes < max_size) {
            meta = s;
            num_segments = 1;
            segments.push_back(remote_path);
        }
    } else {
        if (
          meta.segment_term == s.segment_term
          && meta.size_bytes + s.size_bytes <= max_size) {
            // Cross term merging is disallowed. Because of that we need to stop
            // if the term doesn't match the previous term.
            if (model::next_offset(meta.committed_offset) != s.base_offset) {
                // In case if we're dealing with one of the old manifests
                // with inconsistencies (overlapping offsets, etc).
                num_segments = 0;
                meta = {};
                segments.clear();
                vlog(
                  archival_log.debug,
                  "Reseting the upload, current committed offset: {}, next "
                  "base offset: {}, meta: {}",
                  meta.committed_offset,
                  s.base_offset,
                  meta);
                return false;
            }
            // Move the end of the small segment run forward
            meta.committed_offset = s.committed_offset;
            meta.max_timestamp = s.max_timestamp;
            num_segments++;
            meta.size_bytes += s.size_bytes;
            segments.push_back(remote_path);
        } else {
            return num_segments > 1;
        }
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const adjacent_segment_run& run) {
    std::vector<ss::sstring> names;
    names.reserve(run.segments.size());
    std::transform(
      run.segments.begin(),
      run.segments.end(),
      std::back_inserter(names),
      [](const cloud_storage::remote_segment_path& rsp) {
          return rsp().native();
      });
    fmt::print(
      os,
      "{{meta: {}, num_segments: {}, segments: {}}}",
      run.meta,
      run.num_segments,
      names);
    return os;
}

} // namespace archival
