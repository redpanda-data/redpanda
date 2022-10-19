// Copyright 2020 Redpanda Data, Inc.
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
#include "test_utils/tmp_dir.h"

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

} // namespace

static void run_test_can_append_multiple_flushes(size_t fallocate_size) {
    std::cout.setf(std::ios::unitbuf);
    auto f = open_file("test.segment_appender_random.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);

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

SEASTAR_THREAD_TEST_CASE(test_can_append_multiple_flushes) {
    run_test_can_append_multiple_flushes(16_KiB);
    run_test_can_append_multiple_flushes(32_MiB);
}

static void run_test_can_append_mixed(size_t fallocate_size) {
    auto f = open_file("test_log_segment_mixed.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
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

SEASTAR_THREAD_TEST_CASE(test_can_append_mixed) {
    run_test_can_append_mixed(16_KiB);
    run_test_can_append_mixed(32_MiB);
}

static void run_test_can_append_10MB(size_t fallocate_size) {
    auto f = open_file("test_segment_appender.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);

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
    appender.close().get();
}

SEASTAR_THREAD_TEST_CASE(
  test_can_append_10MB_sequential_write_sequential_read) {
    run_test_can_append_10MB_sequential_write_sequential_read(16_KiB);
    run_test_can_append_10MB_sequential_write_sequential_read(32_MiB);
}

static void run_test_can_append_little_data(size_t fallocate_size) {
    auto f = open_file("test_segment_appender_little.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);
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

SEASTAR_THREAD_TEST_CASE(test_can_append_little_data) {
    run_test_can_append_little_data(16_KiB);
    run_test_can_append_little_data(32_MiB);
}

static void run_test_fallocate_size(size_t fallocate_size) {
    auto f = open_file("test_segment_appender.log");
    storage::storage_resources resources(
      config::mock_binding<size_t>(std::move(fallocate_size)));
    auto appender = make_segment_appender(f, resources);

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
    appender.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_fallocate_size) {
    for (const size_t fallocate_size : {4096ul, 16_KiB, 32_MiB}) {
        run_test_fallocate_size(fallocate_size);
    }
}

/*
 * Assumption about the segment appender:
 *
 *    usage of the appender api is exclusive: you cannot call append or truncate
 *    or any other interface until the future from any previous method
 * completes.
 *
 * The exception to this rule is `flush`, which should be allowed to be invoked
 * asynchronously to append calls, even if an append has not completed:
 *
 *    commit 5668d7da3309d7bd2d03339f4067b64320cb3802
 *    Author: Noah Watkins <noah@vectorized.io>
 *    Date:   Thu Jun 3 15:18:33 2021 -0700
 *
 *        storage: support concurrent append and flushing
 *
 *        This patch adds support to the segment appender for allowing append()
 * to be called before the future from a previous flush() is completed. This new
 * functionality allows raft to pipeline appends while while waiting on flushes
 * to complete asynchronously.
 *
 * It would appear that there is a race condition in which an assertion fails,
 * and this race condition is rare. It requires that truncate or fallocation run
 * in parallel to a slow dma_write.
 *
 * The following test will reproduce the bug on DEBUG builds. Some strategic
 * sleeps may be required to make it reproduce on a RELEASE build.
 */
SEASTAR_THREAD_TEST_CASE(exclusive_concurrent_flushing_drains_all_io) {
    // create an empty file
    temporary_dir dir("exclusive_concurrent_flushing_drains_all_io");
    auto fpath = dir.get_path() / "segment.dat";
    auto f = open_file(fpath.string());

    // appender with 32_KiB fallocation size
    const int falloc_size = 32_KiB;
    storage::storage_resources resources(
      config::mock_binding<size_t>(falloc_size));
    auto appender = make_segment_appender(f, resources);

    const iobuf one_byte = make_random_data(1);
    const iobuf many_bytes = make_random_data(falloc_size * 2);
    std::vector<ss::future<>> bg_append;
    std::vector<ss::future<>> bg_flush;

    /*
     * we want to be able to control when fallocation occurs, and it always
     * occurs on the first append. so let's get that out of the way.
     *
     * after this call the head write-ahead buffer contains 1 byte.
     */
    appender.append(one_byte).get();

    /*
     * FLUSH-A
     *
     * next we want to trigger a write by calling flush. since the head buffer
     * contains 1 byte of dirty data a background write will be started via
     * dispatch_background_head_write. here that is with the important parts:
     *
     *    ss::future<> segment_appender::flush() {
     *
     *       void segment_appender::dispatch_background_head_write() {
     *
     *           // NOT FULL
     *           const auto full = _head->is_full();
     *
     *           // PREV UNITS
     *           auto prev = _prev_head_write;
     *           auto units = ss::get_units(*prev, 1);
     *
     *           // BACKGROUND WRITE
     *           (void)ss::with_semaphore(
     *             _concurrent_flushes,
     *             1,
     *             [units = std::move(units), full]() mutable {
     *                 return units
     *                   .then([this, h, w, start_offset, expected, src, full](
     *                           ssx::semaphore_units u) mutable {
     *                       return _out
     *                         .dma_write(start_offset, src, expected,
     * _opts.priority)
     *
     *
     *           // PREV HEAD _NOT_ RESET
     *           if (full) {
     *               _prev_head_write = ss::make_lw_shared<ssx::semaphore>(1,
     * head_sem_name);
     *           }
     *
     * so in words, at this point there is a background write that is holding 1
     * unit of _concurrent_flushes, and 1 unit of _prev_head_write. finally,
     * since the head buffer was not full, it is also not reset to a fresh mutex
     * before exiting.
     *
     * Note that the flush is start asynchronously in the background which is
     * allowed according to the assumptions stated above.
     */
    bg_flush.push_back(appender.flush());

    /*
     * we want to be able to trigger another background write.  if we called
     * flush again now it would be a noop since the previous dirty data is now
     * in flight ot the disk. so let's add another byte to make the head buffer
     * dirty again.
     */
    appender.append(one_byte).get();

    /*
     * here is the first part of where the race happens. we are going to now to
     * start a new append that is large enough to trigger the fallocation path,
     * and we are going to background the append.
     *
     * this usage is still is still correct according to the assumptions stated
     * above: there is 1 inflight flush and all previous appends have completed.
     *
     * when append is a called the need for fallocation is checked. if an
     * fallocation is needed it is done prior to any remaining data in the
     * append call being written to the current head buffer.
     *
     *    ss::future<> segment_appender::append(const iobuf& io) {
     *
     *       ss::future<> segment_appender::do_next_adaptive_fallocation() {
     *
     *          return ss::with_semaphore(
     *                   _concurrent_flushes,
     *                   ss::semaphore::max_counter(),
     *                   [this, step]() mutable {
     *                       vassert(
     *                         _prev_head_write->available_units() == 1,
     *                         "Unexpected pending head write {}",
     *                         *this);
     *
     * at this point the append we just started should be blocked on
     * _concurrent_flushes because it is trying grab exclusive control
     * (max_counter) AND the background write started from FLUSH-A above is
     * holding 1 unit of this semaphore.
     */
    bg_append.push_back(appender.append(many_bytes));

    /*
     * currently there is 1 flush and 1 append in flight.
     */
    BOOST_REQUIRE(bg_append.size() == 1);
    BOOST_REQUIRE(bg_flush.size() == 1);

    /*
     * now for the final piece of the race. we are going to call flush once
     * more. recall that we have 1 byte of dirty data in the head buffer and
     * many bytes that are in an inflight append, but waiting on fallocate.
     *
     * recall from above what happens when flush starts the background write:
     *
     *    ss::future<> segment_appender::flush() {
     *       void segment_appender::dispatch_background_head_write() {
     *       ...
     *
     *
     * the background write grabs 1 unit of _concurrent_flushes and 1 unit of
     * _prev_head_write. since the head buffer is NOT full, it is not reset to a
     * new semaphore before returning.
     */
    bg_flush.push_back(appender.flush());
    BOOST_REQUIRE(bg_append.size() == 1);
    BOOST_REQUIRE(bg_flush.size() == 2);

    /*
     * now let the race complete.
     *
     * when the background write from the original FLUSH-A completes and
     * releases its unit of _concurrent_flushes and _prev_head_write two things
     * will happen.
     *
     * first, the next waiter on _concurrent_flushes will be woken up. this is
     * the fallocation call from the last append that is in the background.
     *
     * second, _prev_head_write units will be released and control of the mutex
     * will be given to the next waiter which is the background write scheduled
     * from the immediately preceeding flush.
     *
     * when fallocate is scheduled holding exclusive access to concurrent
     * flushes it will find that this assertion doesn't hold because there is a
     * new owner of the current _prev_head_write semaphore.
     *
     *    vassert(
     *      _prev_head_write->available_units() == 1,
     *      "Unexpected pending head write {}",
     *      *this);
     */
    ss::when_all_succeed(
      ss::when_all_succeed(bg_append.begin(), bg_append.end()),
      ss::when_all_succeed(bg_flush.begin(), bg_flush.end()))
      .get();

    appender.close().get();
}
