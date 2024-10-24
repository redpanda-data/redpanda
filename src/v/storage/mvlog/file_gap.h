// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "serde/envelope.h"

#include <cstddef>

namespace storage::experimental::mvlog {

// Serializable representation of a region in a file that is meant to be
// skipped over by readers, e.g. because the file was truncated.
struct file_gap
  : public serde::
      envelope<file_gap, serde::version<0>, serde::compat_version<0>> {
    file_gap(size_t start, size_t len)
      : start_pos(start)
      , length(len) {}

    // The position in the file at which the gap begins.
    size_t start_pos{0};

    // The size of the gap in bytes.
    size_t length{0};
};
using gap_list_t = chunked_vector<file_gap>;

} // namespace storage::experimental::mvlog
