/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "model/limits.h"
#include "model/record_batch_reader.h"
#include "storage/lock_manager.h"
#include "storage/parser.h"
#include "storage/probe.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/util/optimized_optional.hh>

/**
storage/log_reader: use iterator-style traversal for reading

segment 1                                   segment 2
+--+--+--+--+-----+------+--+--+----+---+   +--+--+--+--+-----+---------+
|  |  |  |  |     |      |  |  |    |   |   |  |  |  |  |     |         |
+--+--+--+--+-----+---------+--+----+---+   +--+--+--+--+-----+---------+
^                        ^
|                        |
|                        | log_reader::iterator<continous_batch_parser>
|                        |
|                        +
| log_reader::iterator::reader<log_segment_batch_reader>
|
|
+ log_reader::iterator::next_seg<segment_set::iterator>

Instead of closing the log_segment_batch_reader which reads _one_ segment
after every invocation of `record_batch_reader::load_batches()`
    we keep the pair of iterators around.

    The tradeoff is that we now _must_ close the parser manually
*/
namespace storage {

class log_segment_batch_reader;
class skipping_consumer final : public batch_consumer {
public:
    explicit skipping_consumer(
      log_segment_batch_reader& reader,
      model::timeout_clock::time_point timeout,
      std::optional<model::offset> next_cached_batch) noexcept
      : _reader(reader)
      , _timeout(timeout)
      , _next_cached_batch(next_cached_batch) {}

    consume_result
    accept_batch_start(const model::record_batch_header&) const override;

    void consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t bytes_on_disk) override;

    void skip_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t bytes_on_disk) override;
    void consume_records(iobuf&&) override;
    stop_parser consume_batch_end() override;
    void print(std::ostream&) const override;

private:
    log_segment_batch_reader& _reader;
    model::record_batch_header _header;
    iobuf _records;
    model::timeout_clock::time_point _timeout;
    std::optional<model::offset> _next_cached_batch;
    model::offset _expected_next_batch;
};

class log_segment_batch_reader {
public:
    static constexpr size_t max_buffer_size = 32 * 1024; // 32KB

    log_segment_batch_reader(
      segment&, log_reader_config& config, probe& p) noexcept;
    log_segment_batch_reader(log_segment_batch_reader&&) noexcept = default;
    log_segment_batch_reader&
    operator=(log_segment_batch_reader&&) noexcept = delete;
    log_segment_batch_reader(const log_segment_batch_reader&) = delete;
    log_segment_batch_reader& operator=(const log_segment_batch_reader&)
      = delete;
    ~log_segment_batch_reader() noexcept = default;

    ss::future<result<ss::circular_buffer<model::record_batch>>>
      read_some(model::timeout_clock::time_point);

    ss::future<> close();

private:
    ss::future<std::unique_ptr<continuous_batch_parser>> initialize(
      model::timeout_clock::time_point,
      std::optional<model::offset> next_cached_batch);

    void add_one(model::record_batch&&);

private:
    struct tmp_state {
        ss::circular_buffer<model::record_batch> buffer;
        size_t buffer_size = 0;
        bool is_full() const { return buffer_size >= max_buffer_size; }
    };

    segment& _seg;
    log_reader_config& _config;
    probe& _probe;

    std::unique_ptr<continuous_batch_parser> _iterator;
    tmp_state _state;
    friend class skipping_consumer;
};

class log_reader final : public model::record_batch_reader::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    log_reader(
      std::unique_ptr<lock_manager::lease>, log_reader_config, probe&) noexcept;

    ~log_reader() final {
        vassert(!_iterator.reader, "log reader destroyed with live reader");
    }

    bool is_end_of_stream() const final {
        return _iterator.next_seg == _lease->range.end();
    }

    ss::future<storage_t> do_load_slice(model::timeout_clock::time_point) final;

    ss::future<> finally() noexcept final { return _iterator.close(); }

    void print(std::ostream& os) final {
        fmt::print(os, "storage::log_reader. config {}", _config);
    }

    /**
     * \brief Resets configuration of given reader.
     *
     * Resetting reader configuration allow user to reuse reader. When client
     * request a chunk read it can reuse reader to continue reading given log.
     *
     * f.e.
     * 1. read batches with offsets [0,100]
     * 2. reset configuration with start_offset = 101
     * 3. read next chunk of batches
     */
    void reset_config(log_reader_config cfg) {
        _config = cfg;
        _iterator.next_seg = _iterator.current_reader_seg;
    };

    /**
     * Return next read request lower bound. i.e. lowest offset that can be read
     * using this reader. This way we can match requested offsets with cached
     * readers.
     */
    model::offset next_read_lower_bound() const { return _config.start_offset; }

    /**
     * Base offset of first locked segment in read lock lease
     */
    model::offset lease_range_base_offset() const {
        if (_lease->range.empty()) {
            return model::offset{};
        }
        return _lease->range.front()->offsets().base_offset;
    }
    /**
     * Last offset of last locked segment in read lock lease
     */
    model::offset lease_range_end_offset() const {
        if (_lease->range.empty()) {
            return model::offset{};
        }
        return _lease->range.back()->offsets().dirty_offset;
    }

    /**
     * Indicates if current reader may be reused for future reads.
     *
     * reader reuse is only possible when we have an active reader and we didn't
     * read all locked segments already
     */
    bool is_reusable() const { return _iterator.reader != nullptr; }

private:
    void set_end_of_stream() { _iterator.next_seg = _lease->range.end(); }
    bool is_done();
    ss::future<> find_next_valid_iterator();

private:
    struct iterator_pair {
        iterator_pair(segment_set::iterator i)
          : next_seg(i)
          , current_reader_seg(i) {}
        segment_set::iterator next_seg;
        segment_set::iterator current_reader_seg;
        std::unique_ptr<log_segment_batch_reader> reader = nullptr;

        explicit operator bool() { return bool(reader); }
        ss::future<> close() {
            if (reader) {
                return reader->close().then([this] { reader = nullptr; });
            }
            return ss::make_ready_future<>();
        }

        const auto& offsets() const { return (*next_seg)->offsets(); }
    };

    std::unique_ptr<lock_manager::lease> _lease;
    iterator_pair _iterator;
    log_reader_config _config;
    model::offset _last_base;
    probe& _probe;
    ss::abort_source::subscription _as_sub;
};

/**
 * Assuming caller has already determined that this batch contains
 * the record that should be the result to the timequery, traverse
 * the batch to find which record matches.
 *
 * This is used by both storage's disk_log_impl and by cloud_storage's
 * remote_partition, to seek to their final result after finding
 * the batch.
 */
timequery_result
batch_timequery(const model::record_batch& b, model::timestamp t);

} // namespace storage
