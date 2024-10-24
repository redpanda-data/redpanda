// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/mvlog/file_gap.h"

#include <memory>

template<std::integral>
class interval_set;

namespace storage::experimental::mvlog {

class file;
class segment_reader;

// A readable segment file. This is a long-lived object, responsible for
// passing out short-lived readers.
class readable_segment {
public:
    readable_segment(readable_segment&) = delete;
    readable_segment(readable_segment&&) = delete;
    readable_segment& operator=(readable_segment&) = delete;
    readable_segment& operator=(readable_segment&&) = delete;

    explicit readable_segment(file* f);
    ~readable_segment();

    // Returns a segment reader that reads data as of the time of calling. I.e.
    // honors only gaps that were added before calling, and only reads up to the
    // segment's end as of calling.
    std::unique_ptr<segment_reader> make_reader();

    // Adds the given gap, such that subsequent readers of the file skip the
    // specified range and all those previously added.
    void add_gap(file_gap gap);

    // Returns the number of readers returned created by this segment that have
    // yet to be destructed.
    size_t num_readers() const { return num_readers_; }

private:
    friend class segment_reader;

    file* file_;

    // Set of gaps that have been added to the file. May be null if no gaps
    // have been added yet.
    std::unique_ptr<interval_set<size_t>> gaps_{nullptr};

    size_t num_readers_{0};
};

} // namespace storage::experimental::mvlog
