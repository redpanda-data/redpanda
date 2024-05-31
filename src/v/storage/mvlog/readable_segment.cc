// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/readable_segment.h"

#include "base/vlog.h"
#include "container/interval_set.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/segment_reader.h"

namespace storage::experimental::mvlog {

readable_segment::readable_segment(file* f)
  : file_(f) {}
readable_segment::~readable_segment() = default;

std::unique_ptr<segment_reader> readable_segment::make_reader() {
    return std::make_unique<segment_reader>(
      gaps_ ? *gaps_ : interval_set<size_t>{}, this);
}

void readable_segment::add_gap(file_gap gap) {
    vassert(gap.length > 0, "Expected non-empty gap at pos {}", gap.start_pos);
    vlog(
      log.trace,
      "Adding gap [{}, {})",
      gap.start_pos,
      gap.start_pos + gap.length);
    if (!gaps_) {
        gaps_ = std::make_unique<interval_set<size_t>>();
    }
    auto [_, inserted] = gaps_->insert({gap.start_pos, gap.length});
    vassert(inserted, "Failed to insert gap");
}

} // namespace storage::experimental::mvlog
