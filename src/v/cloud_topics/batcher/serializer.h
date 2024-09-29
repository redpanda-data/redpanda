/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "model/record_batch_reader.h"

namespace experimental::cloud_topics::details {

/// This is a substitute for the placeholder batch.
/// It's needed to produce the actual placeholder
/// batch.
struct lw_placeholder { // NOLINT
    // TODO use stronger types
    lw_placeholder(int32_t, model::offset, uint64_t, uint64_t) noexcept;

    int32_t num_records;
    model::offset base;
    uint64_t size_bytes;
    uint64_t physical_offset;
};

/// Chunk is produced by serializing single record
/// batch reader (which usually contains data from
/// a single produce request). The chunk contains
/// an actual payload (iobuf) and the batch map which
/// is supposed to be used to create placeholder batches
/// (one placeholder is created for every raft_data batch)
struct serialized_chunk {
    serialized_chunk() noexcept = default;
    serialized_chunk(iobuf, chunked_vector<lw_placeholder>) noexcept;

    iobuf payload;
    chunked_vector<lw_placeholder> batches;
};

/// Serialize record batch reader into the iobuf.
///
ss::future<serialized_chunk>
serialize_in_memory_record_batch_reader(model::record_batch_reader rdr);

} // namespace experimental::cloud_topics::details
