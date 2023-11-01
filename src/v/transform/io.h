/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "model/record_batch_reader.h"
#include "model/transform.h"

namespace transform {

/**
 * The output sink for Wasm transforms.
 */
class sink {
public:
    sink() = default;
    sink(const sink&) = delete;
    sink& operator=(const sink&) = delete;
    sink(sink&&) = delete;
    sink& operator=(sink&&) = delete;
    virtual ~sink() = default;

    virtual ss::future<> write(ss::chunked_fifo<model::record_batch>) = 0;
};

/**
 * The input source for Wasm transforms.
 */
class source {
public:
    source() = default;
    source(const source&) = delete;
    source& operator=(const source&) = delete;
    source(source&&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    virtual kafka::offset latest_offset() = 0;
    virtual ss::future<model::record_batch_reader>
    read_batch(kafka::offset, ss::abort_source*) = 0;
};

/**
 * Transforms are at least once delivery, which we achieve by committing
 * progress on the input topic offset periodically.
 */
class offset_tracker {
public:
    offset_tracker() = default;
    offset_tracker(const offset_tracker&) = delete;
    offset_tracker(offset_tracker&&) = delete;
    offset_tracker& operator=(const offset_tracker&) = delete;
    offset_tracker& operator=(offset_tracker&&) = delete;
    virtual ~offset_tracker() = default;

    /**
     * Load the latest offset we've committed.
     */
    virtual ss::future<std::optional<kafka::offset>>
    load_committed_offset() = 0;

    /**
     * Commit progress. The offset here is how far on the input partition we've
     * transformed and successfully written to the output topic.
     */
    virtual ss::future<> commit_offset(kafka::offset) = 0;
};

} // namespace transform
