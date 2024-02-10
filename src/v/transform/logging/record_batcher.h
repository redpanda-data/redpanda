/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record.h"

namespace transform::logging {

namespace detail {
class batcher_impl;
}

/**
 * A class for collecting arbitrarily many key-value pairs (as iobufs) into a
 * collection of model::record_batches, attempting to enforce a maximum size
 * in bytes for each such batch by estimating a running total of serialized
 * record size and rolling over to a new batch when an incoming record would
 * otherwise meet or exceed the specified max.
 *
 * The functions of this class are synchronous and do not perform any I/O.
 */
class record_batcher {
public:
    record_batcher() = delete;
    explicit record_batcher(size_t batch_max_bytes);
    ~record_batcher();
    record_batcher(const record_batcher&) = delete;
    record_batcher& operator=(const record_batcher&) = delete;
    record_batcher(record_batcher&&) = delete;
    record_batcher& operator=(record_batcher&&) = delete;

    /**
     * Add a record to the current batch, possibly rolling over to a
     * new batch if the serialized record would exceed batch_max_bytes.
     */
    void append(iobuf k, iobuf v);

    /**
     * Return the total size in bytes of all record_batches, exclusive of any
     * batch still being built.
     */
    size_t total_size_bytes();

    /**
     * Move the collection of batches from the batcher and leave it in a
     * useable (i.e. empty) state.
     */
    ss::chunked_fifo<model::record_batch> finish();

private:
    std::unique_ptr<detail::batcher_impl> _impl;
};
} // namespace transform::logging
