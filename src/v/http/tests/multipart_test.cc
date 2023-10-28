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

SEASTAR_TEST_CASE(test_reader_empty) {
    auto response_stream = source_stream("");
    auto reader = http::multipart_reader(response_stream, "foobar");

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader_partial_preamble) {
    auto response_stream = source_stream("hello world\r", 1, 1);
    auto reader = http::multipart_reader(response_stream, "foobar");

    BOOST_REQUIRE_EXCEPTION(
      co_await reader.next(),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string_view(e.what())
                 == "incomplete message; state = {preamble}";
      });
}

SEASTAR_TEST_CASE(test_reader_no_body_part) {
    const std::string_view boundary = "foo";
    const std::string_view message = "--foo\r\n"
                                     "\r\n"
                                     "--foo--";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());

    {
        // Extra reads do nothing.
        auto p = co_await reader.next();
        BOOST_TEST_REQUIRE(!p.has_value());
    }

    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader_minimal) {
    const std::string_view boundary = "foo";
    const std::string_view message = "--foo\r\n"
                                     "\r\n"
                                     "single body part\r\n"
                                     "--foo--";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(p.has_value());
    BOOST_TEST_REQUIRE(iobuf_to_string(p->body) == "single body part");

    p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_multiline_preamble) {
    const std::string_view boundary = "foo";
    const std::string_view message = "abra\r\n"
                                     "cadabra\r\n"
                                     "--foo\r\n"
                                     "\r\n"
                                     "single body part\r\n"
                                     "--foo--";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(p.has_value());
    BOOST_TEST_REQUIRE(iobuf_to_string(p->body) == "single body part");

    p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader_trailing_bytes) {
    const std::string_view boundary = "foo";
    const std::string_view message = "--foo\r\n"
                                     "\r\n"
                                     "--foo--\r\n"
                                     "epilogue\r\n"
                                     "trailing bytes\r\n"
                                     "more\r\n"
                                     "\r\n"
                                     "\r\n";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    co_await reader.close();

    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader_trailing_part_crlf) {
    const std::string_view boundary = "foo";
    const std::string_view message = "--foo\r\n"
                                     "\r\n"
                                     "single body part\r\n"
                                     "\r\n\r\n\r\n"
                                     "--foo--";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(p.has_value());
    BOOST_TEST_REQUIRE(
      iobuf_to_string(p->body) == "single body part\r\n\r\n\r\n");

    p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader_fields) {
    const std::string_view boundary = "foo";
    const std::string_view message = "--foo\r\n"
                                     "Content-Type: text/plain\r\n"
                                     "-x-test-folding: Lorem \r\n"
                                     "          ipsum \r\n"
                                     " dolor.\r\n"
                                     "\r\n"
                                     "single body part\r\n"
                                     "--foo--";
    auto response_stream = source_stream(message);
    auto reader = http::multipart_reader(response_stream, boundary);

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(p.has_value());
    BOOST_TEST_CHECK(p->header["content-type"] == "text/plain");
    BOOST_TEST_CHECK(
      p->header["-x-test-folding"] == "Lorem          ipsum dolor.");
    BOOST_TEST_REQUIRE(iobuf_to_string(p->body) == "single body part");

    p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());
    BOOST_TEST_REQUIRE(response_stream.eof());
}

namespace {
// Sample batch response from ABS.
// https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=azure-ad#sample-response
const std::string_view abs_sample_message
  = "409\r\n"
    "boom\r\n"
    "--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed\r\n"
    "Content-Type: application/http\r\n"
    "Content-ID: 0\r\n"
    "\r\n"
    "HTTP/1.1 202 Accepted\r\n"
    "x-ms-delete-type-permanent: true\r\n"
    "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e284f\r\n"
    "x-ms-version: 2018-11-09\r\n"
    "\r\n"
    "--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed\r\n"
    "Content-Type: application/http\r\n"
    "Content-ID: 1\r\n"
    "\r\n"
    "HTTP/1.1 202 Accepted\r\n"
    "x-ms-delete-type-permanent: true\r\n"
    "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2851\r\n"
    "x-ms-version: 2018-11-09\r\n"
    "\r\n"
    "--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed\r\n"
    "Content-Type: application/http\r\n"
    "Content-ID: 2\r\n"
    "\r\n"
    "HTTP/1.1 404 The specified blob does not exist.\r\n"
    "x-ms-error-code: BlobNotFound\r\n"
    "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852\r\n"
    "x-ms-version: 2018-11-09\r\n"
    "Content-Length: 216\r\n"
    "Content-Type: application/xml\r\n"
    "\r\n"
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
    "<Error><Code>BlobNotFound</Code><Message>The specified blob does not "
    "\r\n"
    "exist.\r\n"
    "RequestId:778fdc83-801e-0000-62ff-0334671e2852\r\n"
    "Time:2018-06-14T16:46:54.6040685Z</Message></Error>\r\n"
    "--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed--\r\n"
    "0\r\n"
    "zzz";
static constexpr std::string_view abs_sample_boundary
  = "batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed";
} // namespace

ss::future<> run_abs_expectations(ss::input_stream<char> response_stream) {
    auto reader = http::multipart_reader(response_stream, abs_sample_boundary);

    {
        auto p = co_await reader.next();
        BOOST_TEST_REQUIRE(p.has_value());

        BOOST_TEST_CHECK(num_header_fields(p->header) == 2);
        BOOST_TEST_CHECK(p->header["content-type"] == "application/http");
        BOOST_TEST_CHECK(p->header["content-id"] == "0");
        BOOST_TEST_CHECK(
          iobuf_to_string(p->body)
          == "HTTP/1.1 202 Accepted\r\n"
             "x-ms-delete-type-permanent: true\r\n"
             "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e284f\r\n"
             "x-ms-version: 2018-11-09\r\n"
             "\r\n");
    }

    {
        auto p = co_await reader.next();
        BOOST_TEST_REQUIRE(p.has_value());
        BOOST_TEST_CHECK(num_header_fields(p->header) == 2);
        BOOST_TEST_CHECK(p->header["content-type"] == "application/http");
        BOOST_TEST_CHECK(p->header["content-id"] == "1");
        BOOST_TEST_CHECK(
          iobuf_to_string(p->body)
          == "HTTP/1.1 202 Accepted\r\n"
             "x-ms-delete-type-permanent: true\r\n"
             "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2851\r\n"
             "x-ms-version: 2018-11-09\r\n"
             "\r\n");
    }

    {
        auto p = co_await reader.next();
        BOOST_TEST_REQUIRE(p.has_value());

        BOOST_TEST_CHECK(p->header["content-type"] == "application/http");
        BOOST_TEST_CHECK(p->header["content-id"] == "2");
        BOOST_TEST_CHECK(
          iobuf_to_string(p->body)
          == "HTTP/1.1 404 The specified blob does not exist.\r\n"
             "x-ms-error-code: BlobNotFound\r\n"
             "x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852\r\n"
             "x-ms-version: 2018-11-09\r\n"
             "Content-Length: 216\r\n"
             "Content-Type: application/xml\r\n"
             "\r\n"
             "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
             "<Error><Code>BlobNotFound</Code><Message>The specified blob does "
             "not "
             "\r\n"
             "exist.\r\n"
             "RequestId:778fdc83-801e-0000-62ff-0334671e2852\r\n"
             "Time:2018-06-14T16:46:54.6040685Z</Message></Error>");
    }

    auto p = co_await reader.next();
    BOOST_TEST_REQUIRE(!p.has_value());

    BOOST_TEST_REQUIRE(response_stream.eof());
}

SEASTAR_TEST_CASE(test_reader) {
    auto response_stream = source_stream(abs_sample_message);
    co_await run_abs_expectations(std::move(response_stream));
}

SEASTAR_TEST_CASE(test_reader_byte_by_byte) {
    auto response_stream = source_stream(abs_sample_message, 1, 1);
    co_await run_abs_expectations(std::move(response_stream));
}

namespace {
// This volatile inline assembly statement ensures 'value' is not optimized
// away by creating a data dependency and clobbering memory.
// https://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html#Clobbers-and-Scratch-Registers-1
template<typename T>
void DoNotOptimize(T& value) {
    // NOLINTNEXTLINE(hicpp-no-assembler)
    asm volatile("" : "+m"(value) : : "memory");
}
} // namespace

/// Test that it doesn't hang or crash (due to UB, ASAN, asserts).
SEASTAR_TEST_CASE(test_reader_garbage) {
    for (auto i = 0; i < 100; i++) {
        BOOST_TEST_INFO("Iteration " << i);

        auto response_stream = tests::garbling_input_stream::create(
          abs_sample_message);
        auto reader = http::multipart_reader(
          response_stream, abs_sample_boundary);

        try {
            while (auto p = co_await reader.next()) {
                DoNotOptimize(p);
            }
        } catch (...) {
        }

        co_await reader.close();
    }
}
