/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "model/record.h"

namespace cloud_topics::prefetch {

/// Decodes whole record_batches from a sequence of contiguous byte chunks
/// whose boundaries may fall at arbitrary positions within a batch.
///
/// A "run" inside an L1 object contains only whole batches in L1 on-disk
/// encoding (data_type byte + L1 header + body). feed() accumulates bytes
/// until whole batches are available and returns them; the remaining partial
/// batch bytes ("slack") are carried forward to the next feed() call.
///
/// Call reset() at a run boundary to discard any accumulated slack.
class chunk_reassembler {
public:
    /// Append contiguous bytes; return all whole batches now decodable.
    /// Remaining partial-batch bytes are retained as slack.
    chunked_vector<model::record_batch> feed(iobuf chunk);

    /// Bytes currently carried as incomplete trailing batch.
    size_t slack_bytes() const { return _carry.size_bytes(); }

    /// True when no slack bytes are held (slack_bytes() == 0).
    bool empty() const { return _carry.empty(); }

    /// Discard all carried bytes (call at run boundary).
    void reset() { _carry.clear(); }

private:
    iobuf _carry;
};

} // namespace cloud_topics::prefetch
