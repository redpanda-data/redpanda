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

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "http/multipart.h"
#include "test_utils/iostream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/tools/assertion.hpp>
#include <boost/test/tools/interface.hpp>

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {
ss::input_stream<char>
source_stream(std::string_view message, size_t min = 1, size_t max = 512) {
    return tests::varying_buffer_input_stream::create(message, min, max);
}

ss::sstring iobuf_to_string(iobuf& buf) {
    auto str = ss::uninitialized_string(buf.size_bytes());
    iobuf::iterator_consumer(buf.begin(), buf.end())
      .consume_to(str.size(), str.data());
    return str;
}

size_t num_header_fields(const http::multipart_fields& header) {
    return std::distance(header.begin(), header.end());
}

} // namespace

SEASTAR_TEST_CASE(test_boundary_validation) {
    auto out = ss::output_stream<char>();

    // Invalid characters.
    BOOST_CHECK_EXCEPTION(
      http::multipart_writer(out, "!!!"),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string_view(e.what()) == "invalid boundary";
      });

    // Trailing space.
    BOOST_CHECK_EXCEPTION(
      http::multipart_writer(out, "aaa "),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string_view(e.what()) == "invalid boundary";
      });

    // Too long.
    BOOST_CHECK_EXCEPTION(
      http::multipart_writer(out, std::string(71, 'a')),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string_view(e.what()) == "invalid boundary";
      });

    // Only one test case for the reader, uses the same code under the hood.
    auto in = ss::input_stream<char>();
    BOOST_CHECK_EXCEPTION(
      http::multipart_reader(in, "!!!"),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string_view(e.what()) == "invalid boundary";
      });

    return ss::now();
}

SEASTAR_TEST_CASE(test_writer_empty) {
    auto out = ss::output_stream<char>();
    auto w = http::multipart_writer(out, "foo");

    BOOST_REQUIRE_EXCEPTION(
      co_await w.close(), std::runtime_error, [](const std::runtime_error& e) {
          return std::string_view(e.what())
                 == "RFC2046 requires a multipart message to have at least one "
                    "body part";
      });
}

SEASTAR_TEST_CASE(test_writer_simple) {
    iobuf buf;
    ss::output_stream<char> out = make_iobuf_ref_output_stream(buf);
    auto w = http::multipart_writer(out, "foo");
    auto header = http::multipart_fields{};
    header.insert("content-id", "42");
    header.insert("content-type", "plain/text");
    co_await w.write_part(std::move(header), iobuf{});
    co_await w.close();

    BOOST_TEST_REQUIRE(
      iobuf_to_string(buf)
      == "--foo\r\n"
         "content-id: 42\r\n"
         "content-type: plain/text\r\n"
         "\r\n"
         "\r\n--foo--\r\n");
}

SEASTAR_TEST_CASE(test_writer_multi) {
    iobuf buf;
    ss::output_stream<char> out = make_iobuf_ref_output_stream(buf);
    auto w = http::multipart_writer(out, "foo");
    {
        auto header = http::multipart_fields{};
        header.insert("content-id", "42");
        header.insert("content-type", "plain/text");
        co_await w.write_part(std::move(header), ss::temporary_buffer<char>{});
    }
    {
        auto header = http::multipart_fields{};
        header.insert("content-id", "99");
        header.insert("content-type", "plain/text");
        co_await w.write_part(std::move(header), iobuf{});
    }

    co_await w.close();

    BOOST_TEST_REQUIRE(
      iobuf_to_string(buf)
      == "--foo\r\n"
         "content-id: 42\r\n"
         "content-type: plain/text\r\n"
         "\r\n"
         "\r\n"
         "--foo\r\n"
         "content-id: 99\r\n"
         "content-type: plain/text\r\n"
         "\r\n"
         "\r\n--foo--\r\n");
}

SEASTAR_TEST_CASE(test_writer_multi_with_body) {
    iobuf buf;
    ss::output_stream<char> out = make_iobuf_ref_output_stream(buf);
    auto w = http::multipart_writer(out, "foo");
    {
        static constexpr std::string_view body_content = "hello world";
        auto header = http::multipart_fields{};
        header.insert("content-id", "42");
        header.insert("content-type", "plain/text");
        co_await w.write_part(
          std::move(header), {body_content.data(), body_content.size()});
    }
    {
        // Cover iobuf API too.
        static constexpr std::string_view body_1
          = "HTTP/1.1 200 OK\r\n"
            "Content-Type: plain/text\r\n"
            "\r\n";
        static constexpr std::string_view body_2 = "hello world";
        iobuf buf;
        buf.append(ss::temporary_buffer<char>(body_1.data(), body_1.size()));
        buf.append(ss::temporary_buffer<char>(body_2.data(), body_2.size()));
        auto header = http::multipart_fields{};
        header.insert("content-id", "99");
        header.insert("content-type", "application/html");
        co_await w.write_part(std::move(header), std::move(buf));
    }

    co_await w.close();

    BOOST_TEST_REQUIRE(
      iobuf_to_string(buf)
      == "--foo\r\n"
         "content-id: 42\r\n"
         "content-type: plain/text\r\n"
         "\r\n"
         "hello world\r\n"
         "--foo\r\n"
         "content-id: 99\r\n"
         "content-type: application/html\r\n"
         "\r\n"
         "HTTP/1.1 200 OK\r\n"
         "Content-Type: plain/text\r\n"
         "\r\n"
         "hello world"
         "\r\n--foo--\r\n");
}
