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
#include "jumbo_log/segment/segment.h"
#include "model/fundamental.h"

#include <utility>

namespace jumbo_log::segment::internal {

struct chunk_index_entry {
    model::ntp ntp;
    model::offset offset;

    chunk_loc loc;
};

struct sparse_index_data {
    chunked_vector<jumbo_log::segment::internal::chunk_index_entry> entries;
    std::pair<model::ntp, model::offset> upper_limit;
};

} // namespace jumbo_log::segment::internal
