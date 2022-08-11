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
#include "model/record.h"
#include "outcome.h"
#include "seastarx.h"
#include "storage/exceptions.h"
#include "storage/failure_probes.h"
#include "storage/parser_errc.h"
#include "storage/segment_reader.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/noncopyable_function.hh>

#include <variant>

namespace storage {

class batch_consumer {
public:
    /// \brief  stopping the parser, may or may not be an error condition
    /// it is a public interface indended to signal the internals of the parser
    /// wether to continue or not.
    using stop_parser = ss::bool_class<struct stop_parser_tag>;
    /**
     * Consume results informs parser what it the expected outcome of consume
     * batch start decision
     */
    enum class consume_result : int8_t {
        accept_batch, // accept batch
        stop_parser,  // stop parsing and do not consume batch records
        skip_batch,   // skip given batch without consuming records
    };

    batch_consumer() noexcept = default;
    batch_consumer(const batch_consumer&) = default;
    batch_consumer& operator=(const batch_consumer&) = default;
    batch_consumer(batch_consumer&&) noexcept = default;
    batch_consumer& operator=(batch_consumer&&) noexcept = default;
    virtual ~batch_consumer() noexcept = default;

    /**
     * returns consume result, allow consumer to decide if batch header should
     * be consumed, it may be called more than once with the same header.
     */
    virtual consume_result
    accept_batch_start(const model::record_batch_header&) const = 0;

    /**
     * unconditionally consumes batch start
     */
    virtual void consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk)
      = 0;

    /**
     * unconditionally skip batch
     */
    virtual void skip_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk)
      = 0;

    virtual void consume_records(iobuf&&) = 0;
    virtual stop_parser consume_batch_end() = 0;

    virtual void print(std::ostream&) const = 0;

private:
    friend std::ostream& operator<<(std::ostream& os, const batch_consumer& c) {
        c.print(os);
        return os;
    }
};

class continuous_batch_parser {
public:
    continuous_batch_parser(
      std::unique_ptr<batch_consumer> consumer,
      segment_reader_handle input,
      bool recovery = false) noexcept
      : _consumer(std::move(consumer))
      , _input(std::move(input))
      , _recovery(recovery) {}
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

    /// \brief read and parses header from input_file_stream
    ss::future<result<model::record_batch_header>> read_header();
    /// \brief parses and _stores_ the header into _header variable
    ss::future<result<batch_consumer::stop_parser>> consume_header();

    /// consume the [un]compressed records
    ss::future<result<batch_consumer::stop_parser>> consume_records();

    size_t consumed_batch_bytes() const;
    void add_bytes_and_reset();

    ss::input_stream<char>& get_stream() { return _input.stream(); }

private:
    std::unique_ptr<batch_consumer> _consumer;
    segment_reader_handle _input;

    // If _recovery is true, we do not log unexpected data at the
    // end of the stream as an error (it is expected after an unclean
    // shutdown)
    bool _recovery{false};

    std::optional<model::record_batch_header> _header;
    parser_errc _err = parser_errc::none;
    size_t _bytes_consumed{0};
    size_t _physical_base_offset{0};
};

using record_batch_transform_predicate = ss::noncopyable_function<
  batch_consumer::consume_result(model::record_batch_header&)>;

/// Copy input stream to the output stream
///
/// Invoke predicate for every batch. The predicate can skip or
/// accept the batch. The accepted batches are copied to the output
/// stream. The predicate can also update record batch header in-place
/// or stop the transfer.
///
/// \param in is an input stream with record batches
/// \param out is an output stream that should receive the resulting batches
/// \return number of bytes written to the 'out'
ss::future<result<size_t>> transform_stream(
  ss::input_stream<char> in,
  ss::output_stream<char> out,
  record_batch_transform_predicate pred,
  opt_abort_source_t as = std::nullopt);

} // namespace storage
