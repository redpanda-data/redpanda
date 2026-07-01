/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/prefetch/fetch_stream.h"
#include "cloud_topics/log_reader_config.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

namespace cloud_topics {
class batch_cache;
} // namespace cloud_topics

namespace cloud_topics::prefetch {

/// \brief Cache-only record_batch_reader::impl for the L1 prefetch path.
///
/// Reads exclusively from the shared `batch_cache` (keyed by tidp+offset),
/// without touching cloud storage directly. On a cache miss it pushes demand
/// feedback to its `fetch_stream` and waits for the service to fill the cache.
///
/// The wait resolves on the first of:
///   (a) batch available in cache
///   (b) stream terminal error (woken via fetch_stream::error_abort_source())
///   (c) fetch deadline
///   (d) caller abort / shutdown
///
/// The reader holds a ref on the stream (ref() in ctor, unref() in dtor) and
/// is never cached itself — a new instance is created per fetch.
class memory_first_reader_impl final : public model::record_batch_reader::impl {
public:
    memory_first_reader_impl(
      fetch_stream* stream,
      cloud_topics::batch_cache* cache,
      cloud_topic_log_reader_config cfg);

    ~memory_first_reader_impl() noexcept override;

    memory_first_reader_impl(const memory_first_reader_impl&) = delete;
    memory_first_reader_impl(memory_first_reader_impl&&) = delete;
    memory_first_reader_impl&
    operator=(const memory_first_reader_impl&) = delete;
    memory_first_reader_impl& operator=(memory_first_reader_impl&&) = delete;

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) final;

    fmt::iterator format_to(fmt::iterator it) const final;

private:
    void set_end_of_stream();
    bool is_over_limit_with_bytes(size_t size) const;

    fetch_stream* _stream;
    cloud_topics::batch_cache* _cache;
    cloud_topic_log_reader_config _cfg;
    kafka::offset _next;
    size_t _bytes_consumed{0};
    size_t _reported_bytes{0};
    bool _end_of_stream{false};
};

/// Factory function: construct a record_batch_reader backed by
/// memory_first_reader_impl. The returned reader holds a ref on `stream`
/// via its ctor; the ref is dropped when the reader is destroyed.
model::record_batch_reader make_memory_first_reader(
  fetch_stream* stream,
  cloud_topics::batch_cache* cache,
  cloud_topic_log_reader_config cfg);

} // namespace cloud_topics::prefetch
