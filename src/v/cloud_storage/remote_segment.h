/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "s3/client.h"
#include "storage/parser.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

namespace cloud_storage {

class download_exception : public std::exception {
public:
    explicit download_exception(download_result r, std::filesystem::path p);

    const char* what() const noexcept override;

    const download_result result;
    std::filesystem::path path;
};

class remote_segment_exception : public std::runtime_error {
public:
    explicit remote_segment_exception(const char* m)
      : std::runtime_error(m) {}
};

class remote_segment final {
public:
    remote_segment(
      remote& r,
      cache& cache,
      s3::bucket_name bucket,
      const manifest& m,
      manifest::key path,
      retry_chain_node& parent);

    const model::ntp& get_ntp() const;

    const model::term_id get_term() const;

    const model::offset get_max_offset() const;

    const model::offset get_base_offset() const;

    ss::future<> stop();

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::future<ss::input_stream<char>>
    data_stream(size_t pos, const ss::io_priority_class&);

    /// Hydrate the segment
    ///
    /// Method returns key of the segment in cache.
    /// TODO: implement cache lease system and return a lease instead
    ///       it shouldn't be possible to evict the segment when it's leased
    ss::future<std::filesystem::path> hydrate();

    /// Enter gate
    gate_guard get_gate_guard();

private:
    ss::gate _gate;
    remote& _api;
    cache& _cache;
    s3::bucket_name _bucket;
    const manifest& _manifest;
    manifest::key _path;
    retry_chain_node _rtc;
    mutable retry_chain_logger _ctxlog;
};

class remote_segment_batch_consumer;

/// The segment reader that can be used to fetch data from cloud storage
///
/// The reader invokes 'data_stream' method of the 'remote_segment'
/// which returns hydrated segment from disk.
class remote_segment_batch_reader final {
    friend class remote_segment_batch_consumer;

public:
    // TODO: pass batch-cache
    remote_segment_batch_reader(
      remote_segment&,
      storage::log_reader_config& config,
      model::term_id term) noexcept;

    remote_segment_batch_reader(
      remote_segment_batch_reader&&) noexcept = default;
    remote_segment_batch_reader&
    operator=(remote_segment_batch_reader&&) noexcept = delete;
    remote_segment_batch_reader(const remote_segment_batch_reader&) = delete;
    remote_segment_batch_reader& operator=(const remote_segment_batch_reader&)
      = delete;
    ~remote_segment_batch_reader() noexcept = default;

    ss::future<result<ss::circular_buffer<model::record_batch>>>
      read_some(model::timeout_clock::time_point);

    ss::future<> close();

    model::offset max_offset() const noexcept { return _seg.get_max_offset(); }

    model::offset base_offset() const noexcept {
        return _seg.get_base_offset();
    }

private:
    friend class single_record_consumer;
    ss::future<std::unique_ptr<storage::continuous_batch_parser>> init_parser();

    size_t produce(model::record_batch batch);

    remote_segment& _seg;
    gate_guard _guard;
    storage::log_reader_config& _config;
    std::unique_ptr<storage::continuous_batch_parser> _parser;
    bool _done{false};
    ss::circular_buffer<model::record_batch> _ringbuf;
    size_t _total_size{0};
    model::term_id _term;
};

} // namespace cloud_storage
