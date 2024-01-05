/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/impl/fields.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace http {

using multipart_fields = boost::beast::http::fields;

/// A multipart writer adhering to RFC2046.
/// https://datatracker.ietf.org/doc/html/rfc2046
///
/// \note All methods must be called sequentially. That is, no method may be
/// invoked before the previous method's returned future is resolved.
/// Last call must be to close.
class multipart_writer {
public:
    /// \note The caller must ensure \p out outlives the writer object
    /// constructed here.
    explicit multipart_writer(
      ss::output_stream<char>& out, std::string boundary);
    multipart_writer(multipart_writer&&) = default;
    ~multipart_writer() = default;

    // No compelling reasons for these to exist.
    multipart_writer(const multipart_writer&) = delete;
    multipart_writer& operator=(const multipart_writer&) = delete;
    multipart_writer& operator=(multipart_writer&&) = delete;

    ss::future<>
    write_part(multipart_fields&& header, ss::temporary_buffer<char>&& body);
    ss::future<> write_part(multipart_fields&& header, iobuf&& body);
    ss::future<> close();

private:
    ss::future<> write_delimiter();
    ss::future<> write_part_header(multipart_fields&& header);

private:
    ss::output_stream<char>& _out;
    std::string _boundary;

    bool _first_part_sent = false;
    bool _closed = false;
};

/// A multipart body-part with potentially empty header or body.
/// \note Must be treaded as readonly as the underlying buffers are shared with
/// the reader.
struct multipart_body_part {
    multipart_fields header;
    iobuf body;
};

/// A multipart reader adhering to RFC2046.
/// https://datatracker.ietf.org/doc/html/rfc2046
///
/// \note All methods must be called sequentially. That is, no method may be
/// invoked before the previous method's returned future is resolved.
class multipart_reader {
    enum class parse_state : uint8_t {
        init,
        preamble,
        body_part,
        message_field,
        message_field_name,
        message_field_body,
        message_text,
        body_part_done,
        epilogue,
    };

    friend std::ostream&
    operator<<(std::ostream& o, const multipart_reader::parse_state& r);

    enum class parse_error : uint8_t {
        need_more = 1,
        bad_input,
    };

public:
    /// Construct a multipart reader on top of an input stream.
    ///
    /// \note The caller must ensure \p in outlives the reader object
    /// constructed here.
    /// \note Before destructing the reader, \ref multipart_reader::close()
    /// must be called to drain the stream. \note No assumptions should be
    /// made about the contents of the \p in after this call. However, after
    /// calling \ref multipart_reader::close() it can be assumed that the
    /// input stream is fully consumed.
    multipart_reader(
      ss::input_stream<char>& in, const std::string_view& boundary);
    multipart_reader(multipart_reader&&) = default;
    ~multipart_reader() = default;

    // No compelling reasons for these to exist.
    multipart_reader(const multipart_reader&) = delete;
    multipart_reader& operator=(const multipart_reader&) = delete;
    multipart_reader& operator=(multipart_reader&&) = delete;

    /// Read the next part from the input stream. An empty std::optional is
    /// returned if the entire multipart message is consumed.
    ss::future<std::optional<multipart_body_part>> next();

    /// Drain the input stream without validating any contents.
    ss::future<> close();

private:
    using parse_outcome = checked<parse_state, parse_error>;

    /// Run a parsing step over the internal buffer and returns the next state
    /// or an error to request more data.
    [[nodiscard]] parse_outcome do_parse_step();

    /// Tries to find CRLF in the internal buffer. Since the internal buffer
    /// might be incomplete or end with a LF character, we also return the
    /// position up until which it is guaranteed that no CRLF sequence is
    /// present.
    [[nodiscard]] std::pair<bool, size_t> find_crlf();

    enum class expect_result : uint8_t {
        success,
        failure,
        need_more_data,
    };

    using eat_action = ss::bool_class<struct eat_action_tag>;
    /// Expect that the prefix of the buffer matches.
    [[nodiscard]] expect_result
    expect(std::string_view e, eat_action action = eat_action::no);

    /// Skips bytes from the input.
    void skip(size_t n);

private:
    ss::input_stream<char>& _in;
    std::string _dash_boundary_crlf;
    std::string_view _dash_boundary;

    parse_state _state = parse_state::init;
    iobuf _buf;
    std::string _field_name_scratch;
    std::string _field_value_scratch;
    multipart_body_part _current_part_scratch;
};

/// Generates a random UUID that is safe to be used as a multipart boundary.
std::string random_multipart_boundary();

} // namespace http
