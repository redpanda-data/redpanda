#pragma once

#include "bytes/iobuf.h"
#include "model/limits.h"
#include "model/record_batch_reader.h"
#include "storage/segment_reader.h"
#include "storage/log_set.h"
#include "storage/parser.h"
#include "storage/probe.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/util/optimized_optional.hh>

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

    consume_result consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t bytes_on_disk) override;

    consume_result consume_record(model::record) override;

    void consume_compressed_records(iobuf&&) override;

    stop_parser consume_batch_end() override;

private:
    log_segment_batch_reader& _reader;
    model::record_batch_header _header;
    model::record_batch::records_type _records;
    model::timeout_clock::time_point _timeout;
    std::optional<model::offset> _next_cached_batch;
    model::offset _expected_next_batch;
};

class log_segment_batch_reader {
public:
    static constexpr size_t max_buffer_size = 128 * 1024; // 128KB

    log_segment_batch_reader(
      segment& seg, log_reader_config& config, probe& p) noexcept;
    log_segment_batch_reader(log_segment_batch_reader&&) noexcept = default;
    log_segment_batch_reader& operator=(log_segment_batch_reader&&) noexcept
      = delete;
    log_segment_batch_reader(const log_segment_batch_reader&) = delete;
    log_segment_batch_reader& operator=(const log_segment_batch_reader&)
      = delete;
    ~log_segment_batch_reader() noexcept = default;

    ss::future<ss::circular_buffer<model::record_batch>>
      read(model::timeout_clock::time_point);

private:
    std::unique_ptr<continuous_batch_parser> initialize(
      model::timeout_clock::time_point,
      std::optional<model::offset> next_cached_batch);

    bool is_buffer_full() const;

private:
    segment& _seg;
    log_reader_config& _config;
    probe& _probe;
    ss::circular_buffer<model::record_batch> _buffer;
    size_t _buffer_size = 0;

    friend class skipping_consumer;
};

class log_reader final : public model::record_batch_reader::impl {
public:
    log_reader(log_set&, log_reader_config, probe&) noexcept;

    bool end_of_stream() const final { return _end_of_stream; }

    ss::future<ss::circular_buffer<model::record_batch>>
      do_load_slice(model::timeout_clock::time_point) final;

private:
    bool is_done();

    using reader_available = ss::bool_class<struct create_reader_tag>;

    reader_available maybe_create_segment_reader();

private:
    bool _end_of_stream{false};
    log_set& _set;
    log_reader_config _config;
    model::offset _last_base;
    probe& _probe;
};

} // namespace storage
