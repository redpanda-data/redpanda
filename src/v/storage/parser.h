/*
 * Copyright 2020 Vectorized, Inc.
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
#include "model/record.h"
#include "outcome.h"
#include "seastarx.h"
#include "storage/exceptions.h"
#include "storage/failure_probes.h"
#include "storage/parser_errc.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>

#include <variant>

namespace storage {

class batch_consumer {
public:
    /// \brief this tag is useful for when the user wants to
    /// find a particular offset - think a scan - and we need to skip
    /// the current batch to get to it
    using skip_batch = ss::bool_class<struct skip_batch_tag>;

    /// \brief  stopping the parser, may or may not be an error condition
    /// it is a public interface indended to signal the internals of the parser
    /// wether to continue or not.
    using stop_parser = ss::bool_class<struct stop_parser_tag>;
    using consume_result = std::variant<stop_parser, skip_batch>;

    batch_consumer() noexcept = default;
    batch_consumer(const batch_consumer&) = default;
    batch_consumer& operator=(const batch_consumer&) = default;
    batch_consumer(batch_consumer&&) noexcept = default;
    batch_consumer& operator=(batch_consumer&&) noexcept = default;
    virtual ~batch_consumer() noexcept = default;

    virtual consume_result consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk)
      = 0;

    virtual void consume_records(iobuf&&) = 0;
    virtual stop_parser consume_batch_end() = 0;

    virtual void print(std::ostream&) const = 0;

private:
    friend std::ostream& operator<<(std::ostream&, const batch_consumer&);
};

inline std::ostream& operator<<(std::ostream& os, const batch_consumer& c) {
    c.print(os);
    return os;
}

class continuous_batch_parser {
public:
    continuous_batch_parser(
      std::unique_ptr<batch_consumer> consumer,
      ss::input_stream<char> input) noexcept
      : _consumer(std::move(consumer))
      , _input(std::move(input)) {}
    continuous_batch_parser(const continuous_batch_parser&) = delete;
    continuous_batch_parser& operator=(const continuous_batch_parser&) = delete;
    continuous_batch_parser(continuous_batch_parser&&) noexcept = default;
    continuous_batch_parser&
    operator=(continuous_batch_parser&&) noexcept = default;
    ~continuous_batch_parser() noexcept = default;

    // continues to parse until stop_parser is reached or end of stream
    ss::future<result<size_t>> consume();

    /// \brief cleans up async resources like the input stream
    ss::future<> close() { return _input.close(); }

private:
    /// \brief consumes _one_ full batch.
    ss::future<result<batch_consumer::stop_parser>> consume_one();

    /// \brief parses and _stores_ the header into _header variable
    ss::future<result<batch_consumer::stop_parser>> consume_header();

    /// consume the [un]compressed records
    ss::future<result<batch_consumer::stop_parser>> consume_records();

    size_t consumed_batch_bytes() const;
    void add_bytes_and_reset();

private:
    std::unique_ptr<batch_consumer> _consumer;
    ss::input_stream<char> _input;
    model::record_batch_header _header;
    parser_errc _err = parser_errc::none;
    size_t _bytes_consumed{0};
    size_t _physical_base_offset{0};
};

} // namespace storage
