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
    static constexpr size_t max_buffer_size = 32 * 1024; // 32KB

    log_segment_batch_reader(
      segment&, log_reader_config& config, probe& p) noexcept;
    log_segment_batch_reader(log_segment_batch_reader&&) noexcept = default;
    log_segment_batch_reader& operator=(log_segment_batch_reader&&) noexcept
      = delete;
    log_segment_batch_reader(const log_segment_batch_reader&) = delete;
    log_segment_batch_reader& operator=(const log_segment_batch_reader&)
      = delete;
    ~log_segment_batch_reader() noexcept = default;

    ss::future<result<ss::circular_buffer<model::record_batch>>>
      read_some(model::timeout_clock::time_point);

    ss::future<> close();

private:
    std::unique_ptr<continuous_batch_parser> initialize(
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
    log_reader(
      std::unique_ptr<lock_manager::lease>, log_reader_config, probe&) noexcept;

    bool is_end_of_stream() const final {
        return _iterator.next_seg == _lease->range.end();
    }

    ss::future<ss::circular_buffer<model::record_batch>>
      do_load_slice(model::timeout_clock::time_point) final;

private:
    void set_end_of_stream() { _iterator.next_seg = _lease->range.end(); }
    bool is_done();
    ss::future<> next_iterator();

    using reader_available = ss::bool_class<struct create_reader_tag>;
    reader_available maybe_create_segment_reader();

private:
    struct iterator_pair {
        iterator_pair(segment_set::iterator i)
          : next_seg(i) {}

        segment_set::iterator next_seg;
        std::unique_ptr<log_segment_batch_reader> reader = nullptr;

        explicit operator bool() { return bool(reader); }
        ss::future<> close() {
            if (reader) {
                return reader->close().then([this] { reader = nullptr; });
            }
            return ss::make_ready_future<>();
        }
    };

    std::unique_ptr<lock_manager::lease> _lease;
    iterator_pair _iterator;
    log_reader_config _config;
    model::offset _last_base;
    probe& _probe;
};

} // namespace storage
