// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "jumbo_log/segment/internal.h"
#include "jumbo_log/segment/segment.h"
#include "model/fundamental.h"

#include <absl/types/optional.h>

class sparse_index_data_accessor;

namespace jumbo_log::segment_writer {
class sparse_index_accumulator;
class sparse_index_writer;

} // namespace jumbo_log::segment_writer

namespace jumbo_log::segment_reader {
class sparse_index_reader_from_iobuf;
}

namespace jumbo_log::segment {
class sparse_index {
    friend class ::sparse_index_data_accessor;
    friend class segment_reader::sparse_index_reader_from_iobuf;
    friend class segment_writer::sparse_index_accumulator;
    friend class segment_writer::sparse_index_writer;

private:
    explicit sparse_index(
      jumbo_log::segment::internal::sparse_index_data&& data)
      : _data(std::move(data)) {}

public:
    /// Find the position of the first chunk that might contain the given
    /// NTP and offset.
    std::optional<jumbo_log::segment::chunk_loc>
    find_chunk(const model::ntp& ntp, model::offset offset) const;

private:
    /// O(1) check if the given NTP and offset are within the bounds of the
    /// index.
    bool within_bounds(const model::ntp& ntp, model::offset offset) const;

private:
    jumbo_log::segment::internal::sparse_index_data _data;
};
} // namespace jumbo_log::segment
