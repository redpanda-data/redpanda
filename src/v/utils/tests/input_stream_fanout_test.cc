/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "random/generators.h"
#include "utils/fragmented_vector.h"
#include "utils/stream_utils.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <exception>
#include <vector>

template<typename... T>
auto all_equal(const std::tuple<T...>& t) {
    return std::apply(
      [&t](auto&&... args) { return ((args == std::get<0>(t)) && ...); }, t);
}

template<size_t N>
void test_sync_read(
  size_t readahead, std::optional<size_t> limit = std::nullopt) {
    iobuf input;
    for (int i = 0; i < 20; i++) {
        int sz = random_generators::get_int(100, 32 * 1024);
        auto b = random_generators::get_bytes(sz);
        input.append(bytes_to_iobuf(b));
    }
    auto szfull = input.size_bytes();
    auto is = make_iobuf_input_stream(std::move(input));
    auto streams = input_stream_fanout<N>(std::move(is), readahead, limit);
    static_assert(
      std::tuple_size_v<decltype(streams)> == N,
      "Incorrect number of tuple elements");
    int niter = 0;
    while (true) {
        auto buf = std::apply(
          [](auto&&... s) { return (std::make_tuple(s.read().get()...)); },
          streams);
        BOOST_REQUIRE(all_equal(buf));
        if (std::get<0>(buf).empty()) {
            break;
        }
        niter++;
    }
    if (!limit) {
        BOOST_REQUIRE(
          niter != 0); // Check that there was at least one non empty batch
    } else {
        auto expected_iters = szfull / 1000;
        BOOST_REQUIRE(niter >= expected_iters);
    }
    std::apply([](auto&&... s) { (s.close().get(), ...); }, streams);
}

template<size_t N>
void test_async_read(
  size_t readahead, std::optional<size_t> limit = std::nullopt) {
    // Check the situation when we have one slow consumer
    iobuf input;
    iobuf copy;
    [[maybe_unused]] int szfull = 0;
    for (int i = 0; i < 20; i++) {
        int sz = random_generators::get_int(100, 8 * 1024);
        auto b = random_generators::get_bytes(sz);
        input.append(bytes_to_iobuf(b));
        copy.append(bytes_to_iobuf(b));
        szfull += sz;
    }
    auto is = make_iobuf_input_stream(std::move(input));
    auto streams = input_stream_fanout<N>(std::move(is), readahead, limit);
    ss::gate g;
    int cnt_a = 0;
    auto dispatch_bg_read = [&g, &copy](auto sa) mutable {
        auto cnt = ss::make_lw_shared<int>(0);
        (void)ss::with_gate(g, [&copy, cnt, sa = std::move(sa)]() mutable {
            return ss::async([&copy, cnt, sa = std::move(sa)]() mutable {
                size_t top = 0;
                while (true) {
                    int r = random_generators::get_int(0, 20);
                    if (r == 0) {
                        ss::sleep(std::chrono::milliseconds(5)).get();
                    }
                    auto buf = sa.read().get();
                    if (buf.empty()) {
                        break;
                    }
                    size_t sz = buf.size();
                    iobuf ib;
                    ib.append(std::move(buf));
                    auto actual = iobuf_to_bytes(ib);
                    auto expected = iobuf_to_bytes(copy.share(top, sz));
                    BOOST_REQUIRE(expected == actual);
                    top += sz;
                    ++(*cnt);
                }
                sa.close().get();
                BOOST_REQUIRE_EQUAL(top, copy.size_bytes());
            });
        });
        return cnt;
    };
    auto counters = std::apply(
      [&dispatch_bg_read](auto&&... s) {
          return (std::make_tuple(dispatch_bg_read(std::move(s))...));
      },
      streams);

    /// Wait until all bg jobs are done
    g.close().get();

    auto deref_cnt = std::apply(
      [](auto&&... cnt) { return (std::make_tuple(*cnt...)); }, counters);

    BOOST_REQUIRE(all_equal(deref_cnt));
}

template<typename Head, typename... Tail>
auto split_tuple(std::tuple<Head, Tail...>&& t) {
    struct res_t {
        Head head;
        std::tuple<Tail...> tail;
    };
    return std::apply(
      [](auto&& head, auto&&... tail) {
          return res_t{
            .head = std::forward<Head>(head),
            .tail = std::make_tuple(std::forward<Tail>(tail)...),
          };
      },
      t);
}

template<size_t N>
void test_detached_consumer(
  size_t readahead, std::optional<size_t> limit = std::nullopt) {
    iobuf input;
    for (int i = 0; i < 20; i++) {
        int sz = random_generators::get_int(100, 32 * 1024);
        auto b = random_generators::get_bytes(sz);
        input.append(bytes_to_iobuf(b));
    }
    auto szfull = input.size_bytes();
    auto is = make_iobuf_input_stream(std::move(input));
    auto streams = input_stream_fanout<N>(std::move(is), readahead, limit);
    static_assert(
      std::tuple_size_v<decltype(streams)> == N,
      "Incorrect number of tuple elements");

    auto [head, tail] = split_tuple(std::move(streams));
    // Stop first stream
    head.close().get();

    int niter = 0;
    while (true) {
        auto buf = std::apply(
          [](auto&&... s) { return (std::make_tuple(s.read().get()...)); },
          tail);
        BOOST_REQUIRE(all_equal(buf));
        if (std::get<0>(buf).empty()) {
            break;
        }
        niter++;
    }
    if (!limit) {
        BOOST_REQUIRE(
          niter != 0); // Check that there was at least one non empty batch
    } else {
        auto expected_iters = szfull / 1000;
        BOOST_REQUIRE(niter >= expected_iters);
    }
    std::apply([](auto&&... s) { (s.close().get(), ...); }, tail);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_2) { test_sync_read<2>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_3) { test_sync_read<3>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_4) { test_sync_read<4>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_5) { test_sync_read<5>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_6) { test_sync_read<6>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_7) { test_sync_read<7>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_8) { test_sync_read<8>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_9) { test_sync_read<9>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_10) { test_sync_read<10>(4); }

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_2_size_limit) {
    test_sync_read<2>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_3_size_limit) {
    test_sync_read<3>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_4_size_limit) {
    test_sync_read<4>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_5_size_limit) {
    test_sync_read<5>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_6_size_limit) {
    test_sync_read<6>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_7_size_limit) {
    test_sync_read<7>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_8_size_limit) {
    test_sync_read<8>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_9_size_limit) {
    test_sync_read<9>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_test_10_size_limit) {
    test_sync_read<10>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_2_1) {
    test_async_read<2>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_3_1) {
    test_async_read<3>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_4_1) {
    test_async_read<4>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_5_1) {
    test_async_read<5>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_6_1) {
    test_async_read<6>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_7_1) {
    test_async_read<7>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_8_1) {
    test_async_read<8>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_9_1) {
    test_async_read<9>(1);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_10_1) {
    test_async_read<10>(1);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_2_4) {
    test_async_read<2>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_3_4) {
    test_async_read<3>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_4_4) {
    test_async_read<4>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_5_4) {
    test_async_read<5>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_6_4) {
    test_async_read<6>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_7_4) {
    test_async_read<7>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_8_4) {
    test_async_read<8>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_9_4) {
    test_async_read<9>(4);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_10_4) {
    test_async_read<10>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_2_size_limit) {
    test_async_read<2>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_3_size_limit) {
    test_async_read<3>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_4_size_limit) {
    test_async_read<4>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_5_size_limit) {
    test_async_read<5>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_6_size_limit) {
    test_async_read<6>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_7_size_limit) {
    test_async_read<7>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_8_size_limit) {
    test_async_read<8>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_9_size_limit) {
    test_async_read<9>(4, 1000);
}
SEASTAR_THREAD_TEST_CASE(input_stream_fanout_async_10_size_limit) {
    test_async_read<10>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_2) {
    test_detached_consumer<2>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_3) {
    test_detached_consumer<3>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_4) {
    test_detached_consumer<4>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_5) {
    test_detached_consumer<5>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_6) {
    test_detached_consumer<6>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_7) {
    test_detached_consumer<7>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_8) {
    test_detached_consumer<8>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_9) {
    test_detached_consumer<9>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_10) {
    test_detached_consumer<10>(4);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_2_size_limit) {
    test_detached_consumer<2>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_3_size_limit) {
    test_detached_consumer<3>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_4_size_limit) {
    test_detached_consumer<4>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_5_size_limit) {
    test_detached_consumer<5>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_6_size_limit) {
    test_detached_consumer<6>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_7_size_limit) {
    test_detached_consumer<7>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_8_size_limit) {
    test_detached_consumer<8>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_9_size_limit) {
    test_detached_consumer<9>(4, 1000);
}

SEASTAR_THREAD_TEST_CASE(input_stream_fanout_detach_10_size_limit) {
    test_detached_consumer<10>(4, 1000);
}
