// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "random/generators.h"
#include "seastarx.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// test gate
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/test/tools/interface.hpp>
#include <fmt/format.h>

#include <string_view>

using namespace storage; // NOLINT

namespace {

struct segment_appender_test_accessor {
    segment_appender& sa;
};

segment_appender_test_accessor access(segment_appender& sa) { return {sa}; }

ss::file open_file(std::string_view filename) {
    return ss::open_file_dma(
             filename,
             ss::open_flags::create | ss::open_flags::rw
               | ss::open_flags::truncate)
      .get0();
}

segment_appender
make_segment_appender(ss::file file, storage::storage_resources& resources) {
    return segment_appender(
      std::move(file),
      segment_appender::options(
        ss::default_priority_class(), 1, std::nullopt, resources));
}

iobuf make_random_data(size_t len) {
    return bytes_to_iobuf(random_generators::get_bytes(len));
}

// fill an iobuf with len copies of char c
iobuf make_iobuf_with_char(size_t len, unsigned char c) {
    auto buf = ss::uninitialized_string<bytes>(len);
    std::memset(buf.data(), c, len);
    iobuf ret;
    ret.append(buf.data(), buf.size());
    return ret;
}

} // namespace

static void run_test_can_append_multiple_flushes(size_t fallocate_size) {
    std::cout.setf(std::ios::unitbuf);
    auto f = open_file("test.segment_appender_random.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });

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
}

SEASTAR_THREAD_TEST_CASE(test_can_append_multiple_flushes) {
    run_test_can_append_multiple_flushes(16_KiB);
    run_test_can_append_multiple_flushes(32_MiB);
}

static void run_test_can_append_mixed(size_t fallocate_size) {
    auto f = open_file("test_log_segment_mixed.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });
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
}

SEASTAR_THREAD_TEST_CASE(test_can_append_mixed) {
    run_test_can_append_mixed(16_KiB);
    run_test_can_append_mixed(32_MiB);
}

static void run_test_can_append_10MB(size_t fallocate_size) {
    auto f = open_file("test_segment_appender.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });

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
}

SEASTAR_THREAD_TEST_CASE(test_can_append_10MB) {
    run_test_can_append_10MB(16_KiB);
    run_test_can_append_10MB(32_MiB);
}

static void run_test_can_append_10MB_sequential_write_sequential_read(
  size_t fallocate_size) {
    auto f = open_file("test_segment_appender_sequential.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });

    // write sequential. then read all
    constexpr size_t one_meg = 1024 * 1024;
    // issue #4077: why didn't this fail when I passed in one_meg?
    // I'd expect making the input stream or read..exactly to fail.
    iobuf original = make_random_data(10 * one_meg);
    appender.append(original).get();
    appender.flush().get();
    for (size_t i = 0; i < 10; ++i) {
        auto in = make_file_input_stream(f, i * one_meg);
        iobuf result = read_iobuf_exactly(in, one_meg).get0();
        iobuf tmp_o = original.share(i * one_meg, one_meg);
        // read_iobuf_exactly can return a short read, but we do not expect that
        // here.
        BOOST_REQUIRE_EQUAL(one_meg, result.size_bytes());
        BOOST_REQUIRE_EQUAL(tmp_o, result);
        in.close().get();
    }
}

SEASTAR_THREAD_TEST_CASE(
  test_can_append_10MB_sequential_write_sequential_read) {
    run_test_can_append_10MB_sequential_write_sequential_read(16_KiB);
    run_test_can_append_10MB_sequential_write_sequential_read(32_MiB);
}

static void run_concurrent_append_flush(
  size_t fallocate_size,
  const size_t max_buf_size,
  const size_t buf_count = 10000) {
    auto filename = fmt::format(
      "run_concurrent_append_flush_{}.log", fallocate_size);
    auto f = open_file(filename);
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });

    std::vector<iobuf> bufs(buf_count);
    unsigned char v = 1;
    for (auto& buf : bufs) {
        buf = make_iobuf_with_char(random_generators::get_int(max_buf_size), v);
        if (++v == 0) {
            v = 1;
        }
    }

    // we do one of the following actions with equal probability,
    // respecting the rule that any previous append must have resolved before a
    // new one is invoked
    enum action { APPEND, FLUSH, WAIT_APPEND, YIELD, LAST };

    std::optional<ss::future<>> last_append;
    std::vector<ss::future<>> futs;

    for (size_t buf_index = 0; buf_index < bufs.size();) {
        auto next_action = (action)random_generators::get_int(LAST - 1);

        switch (next_action) {
        case APPEND:
            if (!last_append) { // only if the previous append has finished
                last_append = appender.append(bufs[buf_index++]);
            }
            break;
        case FLUSH:
            futs.push_back(appender.flush());
            break;
        case WAIT_APPEND:
            if (last_append) {
                last_append->get();
                last_append.reset();
            }
            break;
        case YIELD:
            ss::yield().get();
            break;
        default:
            BOOST_TEST_FAIL("bad action");
        }
    }

    // finally we need to wait for the last append, if any
    if (last_append) {
        last_append->get();
    }
    futs.push_back(appender.flush()); // and do a final flush

    for (auto& f : futs) {
        f.get();
    }
    auto in = make_file_input_stream(f);
    auto closefile = ss::defer([&] { in.close().get(); });
    for (auto& buf : bufs) {
        size_t sz = buf.size_bytes();
        iobuf result = read_iobuf_exactly(in, sz).get();
        BOOST_REQUIRE_EQUAL(buf, result);
    }
}

SEASTAR_THREAD_TEST_CASE(test_concurrent_append_flush) {
    // we use smaller buffer counts for the large buffer size tests
    // to keep the runtime manageable (less than ~2 seconds for this test)
    run_concurrent_append_flush(16_KiB, 1);
    run_concurrent_append_flush(16_KiB, 1000);
    run_concurrent_append_flush(16_KiB, 20000, 100);

    run_concurrent_append_flush(64_KiB, 1000);

    run_concurrent_append_flush(32_MiB, 1000, 1000);
}

static void run_test_can_append_little_data(size_t fallocate_size) {
    auto f = open_file("test_segment_appender_little.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });
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
}

SEASTAR_THREAD_TEST_CASE(test_can_append_little_data) {
    run_test_can_append_little_data(16_KiB);
    run_test_can_append_little_data(32_MiB);
}

static void run_test_fallocate_size(size_t fallocate_size) {
    auto f = open_file("test_segment_appender.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
    auto close = ss::defer([&appender] { appender.close().get(); });

    for (size_t i = 0; i < 10; ++i) {
        iobuf original;
        constexpr size_t message_size = 123;
        constexpr size_t messages_amount = 100;
        constexpr size_t one_meg = message_size * messages_amount;
        {
            const auto data = random_generators::gen_alphanum_string(
              message_size);
            for (size_t i = 0; i < messages_amount; ++i) {
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
}

SEASTAR_THREAD_TEST_CASE(test_fallocate_size) {
    for (const size_t fallocate_size : {4096ul, 16_KiB, 32_MiB}) {
        run_test_fallocate_size(fallocate_size);
    }
}
