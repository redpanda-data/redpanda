#pragma once

#include "bytes/iobuf.h"
#include "model/limits.h"
#include "model/record_batch_reader.h"
#include "storage/log_segment_reader.h"
#include "storage/offset_tracker.h"
#include "storage/parser.h"
#include "storage/probe.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/optimized_optional.hh>

namespace storage {

/**
 * Log reader configuration.
 *
 * The default reader configuration will read all batch types. To filter batches
 * by type add the types of interest to the type_filter set.
 *
 * The type filter is sorted before a segment scan, and a linear search is
 * performed. This will generally perform better than something like a binary
 * search when the size of the filter set is small (e.g. < 5). If you need to
 * use a larger filter then this design should be revisited.
 */
struct log_reader_config {
    model::offset start_offset;
    size_t max_bytes;
    size_t min_bytes;
    ss::io_priority_class prio;
    std::vector<model::record_batch_type> type_filter;
    model::offset max_offset
      = model::model_limits<model::offset>::max(); // inclusive
};

class log_segment_batch_reader;

class skipping_consumer : public batch_consumer {
public:
    explicit skipping_consumer(
      log_segment_batch_reader& reader, model::offset start_offset) noexcept
      : _reader(reader)
      , _start_offset(start_offset) {}

    void set_timeout(model::timeout_clock::time_point timeout) {
        _timeout = timeout;
    }

    skip consume_batch_start(
      model::record_batch_header, size_t num_records) override;

    skip consume_record_key(
      size_t size_bytes,
      model::record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key) override;

    void consume_record_value(iobuf&&) override;

    void consume_compressed_records(iobuf&&) override;

    ss::stop_iteration consume_batch_end() override;

private:
    bool skip_batch_type(model::record_batch_type type);

    log_segment_batch_reader& _reader;
    model::offset _start_offset;
    model::record_batch_header _header;
    size_t _record_size_bytes{0};
    model::record_attributes _record_attributes;
    int32_t _record_timestamp_delta{0};
    int32_t _record_offset_delta{0};
    iobuf _record_key;
    size_t _num_records{0};
    model::record_batch::records_type _records;
    model::timeout_clock::time_point _timeout;
};

class log_segment_batch_reader : public model::record_batch_reader::impl {
    static constexpr size_t max_buffer_size = 8 * 1024;
    using span = model::record_batch_reader::impl::span;

public:
    log_segment_batch_reader(
      segment_reader_ptr seg,
      offset_tracker& tracker,
      log_reader_config config,
      probe& probe) noexcept;
    log_segment_batch_reader(log_segment_batch_reader&&) noexcept = default;
    log_segment_batch_reader(const log_segment_batch_reader&) = delete;
    log_segment_batch_reader operator=(const log_segment_batch_reader&)
      = delete;
    size_t bytes_read() const { return _bytes_read; }

protected:
    ss::future<span> do_load_slice(model::timeout_clock::time_point) override;

private:
    ss::future<> initialize();

    bool is_initialized() const;

    bool is_buffer_full() const;

    /// reset_state() allows further reads to happen on a reader
    /// that previously reached the end of the stream. Useful to
    /// implement cached readers that can continue a read where
    /// it left off.
    void reset_state();

    friend class log_reader;

private:
    segment_reader_ptr _seg;
    offset_tracker& _tracker;
    log_reader_config _config;
    size_t _bytes_read = 0;
    skipping_consumer _consumer;
    ss::input_stream<char> _input;
    continuous_batch_parser_opt _parser;
    std::vector<model::record_batch> _buffer;
    size_t _buffer_size = 0;
    bool _over_committed_offset = false;
    probe& _probe;

    friend class skipping_consumer;
};

class log_reader : public model::record_batch_reader::impl {
public:
    using span = model::record_batch_reader::impl::span;

    // Note: The offset tracker will contain base offsets,
    //       and will never point to an offset within a
    //       batch, that is, of an individual record. This
    //       is because batchs are atomically made visible.
    log_reader(log_set&, offset_tracker&, log_reader_config, probe&) noexcept;

    ss::future<span> do_load_slice(model::timeout_clock::time_point) override;

private:
    bool is_done();

    using reader_available = ss::bool_class<struct create_reader_tag>;

    reader_available maybe_create_segment_reader();

private:
    log_segment_selector _selector;
    offset_tracker& _offset_tracker;
    log_reader_config _config;
    probe& _probe;
    std::optional<log_segment_batch_reader> _current_reader;
};

} // namespace storage
