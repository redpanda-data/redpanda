#include "bytes/iobuf.h"
#include "random/generators.h"
#include "storage/log_segment_appender.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// test gate
#include <seastar/core/gate.hh>

#include <fmt/format.h>

using namespace storage; // NOLINT

SEASTAR_THREAD_TEST_CASE(test_can_append_mixed) {
    auto f = ss::open_file_dma(
               "test_log_segment_mixed.log",
               ss::open_flags::create | ss::open_flags::rw
                 | ss::open_flags::truncate)
               .get0();
    auto appender = log_segment_appender(
      f, log_segment_appender::options(ss::default_priority_class()));

    for (size_t i = 0, acc = 0; i < 100; ++i) {
        iobuf original;
        const size_t step = random_generators::get_int<size_t>(
                              0, appender.dma_write_alignment() * 2)
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
    auto f = ss::open_file_dma(
               "test_log_segment_appender.log",
               ss::open_flags::create | ss::open_flags::rw
                 | ss::open_flags::truncate)
               .get0();
    auto appender = log_segment_appender(
      f, log_segment_appender::options(ss::default_priority_class()));

    for (size_t i = 0; i < 10; ++i) {
        iobuf original;
        constexpr size_t one_meg = 1024 * 1024;
        {
            const auto data = random_generators::gen_alphanum_string(1024);
            for (size_t i = 0; i < 1024; ++i) {
                original.append(data.data(), data.size());
            }
        }
        BOOST_CHECK_EQUAL(one_meg, original.size_bytes());
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
    auto f = ss::open_file_dma(
               "test_log_segment_appender_sequential.log",
               ss::open_flags::create | ss::open_flags::rw
                 | ss::open_flags::truncate)
               .get0();
    auto appender = log_segment_appender(
      f, log_segment_appender::options(ss::default_priority_class()));

    // write sequential. then read all
    iobuf original;
    constexpr size_t one_meg = 1024 * 1024;
    {
        const auto data = random_generators::gen_alphanum_string(1024);
        for (size_t i = 0; i < 1024 * 10; ++i) {
            original.append(data.data(), data.size());
        }
    }
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
    auto f = ss::open_file_dma(
               "test_log_segment_appender_little.log",
               ss::open_flags::create | ss::open_flags::rw
                 | ss::open_flags::truncate)
               .get0();
    auto appender = log_segment_appender(
      f, log_segment_appender::options(ss::default_priority_class()));

    // at least 1 page and some 20 bytes to test boundary conditions
    const auto data = random_generators::gen_alphanum_string(
      appender.dma_write_alignment() + 20);
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
