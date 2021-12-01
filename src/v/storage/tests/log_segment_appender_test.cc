// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "random/generators.h"
#include "seastarx.h"
#include "storage/segment_appender.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// test gate
#include <seastar/core/gate.hh>

#include <fmt/format.h>

#include <string_view>

using namespace storage; // NOLINT

namespace {

ss::file open_file(std::string_view filename) {
    return ss::open_file_dma(
             filename,
             ss::open_flags::create | ss::open_flags::rw
               | ss::open_flags::truncate)
      .get0();
}

segment_appender make_segment_appender(ss::file file) {
    return segment_appender(
      std::move(file),
      segment_appender::options(ss::default_priority_class(), 1));
}

iobuf make_random_data(size_t len) {
    size_t random_len = std::min(1024UL, len); // NOLINT local constant
    const auto rbuf = random_generators::gen_alphanum_string(random_len);
    iobuf output;
    size_t left = len;
    while (left > 0) {
        size_t copy_len = std::min(left, random_len);
        output.append(rbuf.data(), copy_len);
        left -= copy_len;
    }
    BOOST_CHECK_EQUAL(len, output.size_bytes());
    return output;
}
} // namespace

SEASTAR_THREAD_TEST_CASE(test_can_append_multiple_flushes) {
    std::cout.setf(std::ios::unitbuf);
    auto f = open_file("test.segment_appender_random.log");
    auto appender = make_segment_appender(f);

    iobuf expected;
    ss::sstring data = "123456789\n";
    for (size_t i = 0; i < 10; ++i) {
        for (int j = 0; j < 910; ++j) {
            expected.append(data.data(), data.size());
            appender.append(data.data(), data.size()).get();
        }
        // This 911 time of appending "_redpanda" causes bug
        // Commnting next two lines make the test passing
        expected.append(data.data(), data.size());
        appender.append(data.data(), data.size()).get();
        appender.flush().get();

        expected.append(data.data(), data.size());
        appender.append(data.data(), data.size()).get();
        appender.flush().get();

        auto in = make_file_input_stream(f, 0);
        iobuf result = read_iobuf_exactly(in, expected.size_bytes()).get0();
        BOOST_REQUIRE_EQUAL(result.size_bytes(), expected.size_bytes());
        BOOST_REQUIRE_EQUAL(result, expected);
        in.close().get();
    }
    appender.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_can_append_mixed) {
    auto f = open_file("test_log_segment_mixed.log");
    auto appender = make_segment_appender(f);
    auto alignment = f.disk_write_dma_alignment();
    for (size_t i = 0, acc = 0; i < 100; ++i) {
        iobuf original;
        const size_t step = random_generators::get_int<size_t>(0, alignment * 2)
                            + 1;
        {
            const auto data = random_generators::gen_alphanum_string(step - 1);
            original.append(data.data(), data.size());
            original.append("\n", 1);
        }
        BOOST_REQUIRE_EQUAL(step, original.size_bytes());
        appender.append(original).get();
        appender.flush().get();
        BOOST_REQUIRE_EQUAL(acc + step, appender.file_byte_offset());
        auto in = make_file_input_stream(f, acc);
        iobuf result = read_iobuf_exactly(in, step).get0();
        fmt::print(
          "==> i:{}, step:{}, acc:{}, og.size:{}, expected.size{}\n",
          i,
          step,
          acc,
          original.size_bytes(),
          result.size_bytes());
        if (original != result) {
            auto in = iobuf::iterator_consumer(
              original.cbegin(), original.cend());
            in.consume(original.size_bytes(), [](const char* src, size_t n) {
                fmt::print("\nOriginal\n");
                while (n-- > 0) {
                    fmt::print("{}", *src++);
                }
                fmt::print("\n");
                return ss::stop_iteration::no;
            });
            in = iobuf::iterator_consumer(result.cbegin(), result.cend());
            in.consume(original.size_bytes(), [](const char* src, size_t n) {
                fmt::print("\nResult\n");
                while (n-- > 0) {
                    fmt::print("{}", *src++);
                }
                fmt::print("\n");
                return ss::stop_iteration::no;
            });

            // fail the test
            BOOST_REQUIRE_EQUAL(original, result);
        }
        acc += step;
        in.close().get();
    }
    appender.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_can_append_10MB) {
    auto f = open_file("test_segment_appender.log");
    auto appender = make_segment_appender(f);

    for (size_t i = 0; i < 10; ++i) {
        constexpr size_t one_meg = 1024 * 1024;
        iobuf original = make_random_data(one_meg);
        appender.append(original).get();
        appender.flush().get();

        auto in = make_file_input_stream(f, i * one_meg);
        iobuf result = read_iobuf_exactly(in, one_meg).get0();
        BOOST_CHECK_EQUAL(original, result);
        in.close().get();
    }
    appender.close().get();
}
SEASTAR_THREAD_TEST_CASE(
  test_can_append_10MB_sequential_write_sequential_read) {
    auto f = open_file("test_segment_appender_sequential.log");
    auto appender = make_segment_appender(f);

    // write sequential. then read all
    constexpr size_t one_meg = 1024 * 1024;
    iobuf original = make_random_data(one_meg);
    appender.append(original).get();
    appender.flush().get();
    for (size_t i = 0; i < 10; ++i) {
        auto in = make_file_input_stream(f, i * one_meg);
        iobuf result = read_iobuf_exactly(in, one_meg).get0();
        iobuf tmp_o = original.share(i * one_meg, one_meg);
        BOOST_REQUIRE_EQUAL(tmp_o, result);
        in.close().get();
    }
    appender.close().get();
}
SEASTAR_THREAD_TEST_CASE(test_can_append_little_data) {
    auto f = open_file("test_segment_appender_little.log");
    auto appender = make_segment_appender(f);
    auto alignment = f.disk_write_dma_alignment();
    // at least 1 page and some 20 bytes to test boundary conditions
    const auto data = random_generators::gen_alphanum_string(alignment + 20);
    for (size_t i = 0; i < data.size(); ++i) {
        char c = data[i];
        appender.append(&c, 1).get();
        appender.flush().get();
        auto in = make_file_input_stream(f, i);
        auto result = in.read_exactly(1).get0();
        if (c != result[0]) {
            std::vector<char> tmp;
            tmp.reserve(7);
            std::copy(
              data.begin() + std::min<size_t>(i, i - 3),
              data.begin() + i,
              std::back_inserter(tmp));
            std::copy(
              data.begin() + i,
              data.begin() + std::min<size_t>(data.size(), i + 3),
              std::back_inserter(tmp));
            tmp.push_back('\0');
            fmt::print("\nINPUT AROUND:{}, i:{}\n", tmp.data(), i);
            // make it fail
            BOOST_REQUIRE_EQUAL(c, result[0]);
        }
        in.close().get();
    }
    BOOST_REQUIRE_EQUAL(appender.file_byte_offset(), data.size());
    appender.close().get();
}
