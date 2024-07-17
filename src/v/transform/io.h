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

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /**
     * The last offset of a record the log - if the log is empty then
     * `kafka::offset::min()` is returned.
     */
    virtual kafka::offset latest_offset() = 0;

    /**
     * The offset of a record the log for a given timestamp - if the log is
     * empty or the timestamp is greater than max_timestamp, then std::nullopt
     * is returned.
     */
    virtual ss::future<std::optional<kafka::offset>>
    offset_at_timestamp(model::timestamp, ss::abort_source*) = 0;

    /**
     * The minimum offset in the source log
     */
    virtual kafka::offset start_offset() const = 0;

    /**
     * Read from the log starting at a given offset, aborting when requested.
     *
     * NOTE: It's important in terms of lifetimes that the source **always**
     * outlives any reader returned from this method.
     *
     * NOTE: It's not valid to have pending futures outstanding from this
     * method before calling stop.
     */
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

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /**
     * When processors are moved, it's possible to still have pending flushes
     * outstanding, so wait for those to be flushed.
     *
     * For more discussion on this method, see
     * `commit_batcher::wait_for_previous_flushes`.
     */
    virtual ss::future<> wait_for_previous_flushes(ss::abort_source*) = 0;

    /**
     * Load the latest offset for all output topics we've committed.
     */
    virtual ss::future<
      absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_committed_offsets() = 0;

    /**
     * Commit progress for a given output topic. The offset here is how far on
     * the input partition we've transformed and successfully written to the
     * output topic.
     */
    virtual ss::future<>
      commit_offset(model::output_topic_index, kafka::offset) = 0;
};

} // namespace transform
