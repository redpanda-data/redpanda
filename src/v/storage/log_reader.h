#pragma once

#include "bytes/iobuf.h"
#include "model/limits.h"
#include "model/record_batch_reader.h"
#include "storage/log_segment_reader.h"
#include "storage/log_set.h"
#include "storage/parser.h"
#include "storage/probe.h"
#include "storage/types.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/optimized_optional.hh>

namespace storage {

class log_segment_batch_reader;
class skipping_consumer final : public batch_consumer {
public:
    explicit skipping_consumer(
      log_segment_batch_reader& reader,
      model::timeout_clock::time_point timeout) noexcept
      : _reader(reader)
      , _timeout(timeout) {}

    consume_result consume_batch_start(
      model::record_batch_header,
      size_t num_records,
      size_t physical_base_offset,
      size_t bytes_on_disk) override;

    consume_result consume_record(
      size_t size_bytes,
      model::record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key,
      iobuf&& headers_and_value) override;

    void consume_compressed_records(iobuf&&) override;

    stop_parser consume_batch_end() override;

private:
    bool skip_batch_type(model::record_batch_type type);

    log_segment_batch_reader& _reader;
    model::record_batch_header _header;
    size_t _num_records{0};
    model::record_batch::records_type _records;
    model::timeout_clock::time_point _timeout;
};

class log_segment_batch_reader {
public:
    static constexpr size_t max_buffer_size = 32 * 1024; // 32KB

    log_segment_batch_reader(
      segment& seg,
      log_reader_config& config,
      std::vector<model::record_batch>& recs) noexcept;
    log_segment_batch_reader(log_segment_batch_reader&&) noexcept = default;
    log_segment_batch_reader(const log_segment_batch_reader&) = delete;
    log_segment_batch_reader operator=(const log_segment_batch_reader&)
      = delete;

    ss::future<size_t> read(model::timeout_clock::time_point);

private:
    std::unique_ptr<continuous_batch_parser>
      initialize(model::timeout_clock::time_point);

    bool is_buffer_full() const;

private:
    segment& _seg;
    log_reader_config& _config;
    std::vector<model::record_batch>& _buffer;
    size_t _buffer_size = 0;

    friend class skipping_consumer;
};

class log_reader : public model::record_batch_reader::impl {
public:
    using span = model::record_batch_reader::impl::span;

    log_reader(log_set&, log_reader_config, probe&) noexcept;

    ss::future<span> do_load_slice(model::timeout_clock::time_point) override;

private:
    bool is_done();

    using reader_available = ss::bool_class<struct create_reader_tag>;

    reader_available maybe_create_segment_reader();

private:
    log_set& _set;
    log_reader_config _config;
    std::vector<model::record_batch> _batches;
    model::offset _last_base;
    probe& _probe;
    bool _seen_first_batch;
};

} // namespace storage
