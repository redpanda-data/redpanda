#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/exceptions.h"
#include "storage/failure_probes.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>

#include <variant>

namespace storage {

class batch_consumer {
public:
    using skip_batch = ss::bool_class<struct skip_batch_tag>;
    using stop_parser = ss::bool_class<struct stop_parser_tag>;
    using consume_result = std::variant<stop_parser, skip_batch>;

    virtual ~batch_consumer() = default;

    virtual consume_result consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk)
      = 0;

    virtual consume_result consume_record(model::record) = 0;
    virtual void consume_compressed_records(iobuf&&) = 0;
    virtual stop_parser consume_batch_end() = 0;
};

class continuous_batch_parser {
public:
    continuous_batch_parser(
      std::unique_ptr<batch_consumer> consumer,
      ss::input_stream<char> input) noexcept
      : _consumer(std::move(consumer))
      , _input(std::move(input)) {}
    continuous_batch_parser(continuous_batch_parser&&) = default;
    continuous_batch_parser& operator=(continuous_batch_parser&&) = default;
    ~continuous_batch_parser() = default;

    // continues to parse until stop_parser is reached or end of stream
    ss::future<size_t> consume();
    bool eof() const { return _input.eof(); }
    ss::future<> close() { return _input.close(); }

private:
    ss::future<batch_consumer::stop_parser> consume_one();
    ss::future<batch_consumer::stop_parser> consume_header();
    bool is_compressed_payload() const;
    ss::future<batch_consumer::stop_parser> consume_compressed_records();
    ss::future<batch_consumer::stop_parser> consume_records();
    size_t consumed_batch_bytes() const;
    void add_bytes_and_reset();

private:
    std::unique_ptr<batch_consumer> _consumer;
    ss::input_stream<char> _input;
    model::record_batch_header _header;
    size_t _bytes_consumed{0};
    size_t _physical_base_offset{0};
};

} // namespace storage
