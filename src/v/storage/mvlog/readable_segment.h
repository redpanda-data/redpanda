// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include <memory>

namespace experimental::io {
class pager;
}

namespace storage::experimental::mvlog {

class segment_reader;

// A readable segment file. This is a long-lived object, responsible for
// passing out short-lived readers.
class readable_segment {
public:
    explicit readable_segment(::experimental::io::pager* pager)
      : pager_(pager) {}

    std::unique_ptr<segment_reader> make_reader();
    size_t num_readers() const { return num_readers_; }

private:
    friend class segment_reader;

    ::experimental::io::pager* pager_;

    size_t num_readers_{0};
};

} // namespace storage::experimental::mvlog
