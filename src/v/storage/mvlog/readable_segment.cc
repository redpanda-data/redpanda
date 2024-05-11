// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/readable_segment.h"

#include "storage/mvlog/segment_reader.h"

namespace storage::experimental::mvlog {

std::unique_ptr<segment_reader> readable_segment::make_reader() {
    return std::make_unique<segment_reader>(this);
}

} // namespace storage::experimental::mvlog
