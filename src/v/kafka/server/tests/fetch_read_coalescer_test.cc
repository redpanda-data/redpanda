/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/iobuf.h"
#include "config/mock_property.h"
#include "kafka/server/fetch_read_coalescer.h"
#include "kafka/server/handlers/fetch.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace kafka {

namespace {

fetch_read_coalescer::cache_config test_cache_config() {
    return {.cache_size = 8, .small_size = 1};
}

coalesce_key make_key(
  model::offset offset = model::offset{5},
  size_t max_bytes = 1024,
  model::isolation_level level = model::isolation_level::read_uncommitted,
  bool obligatory = true) {
    return {
      model::ktp_with_hash{"topic", 0}, offset, level, max_bytes, obligatory};
}

// A data-bearing result with `bytes` of payload, read at the given bounds.
read_result make_result(
  size_t bytes = 7,
  model::offset hwm = model::offset{100},
  model::offset lso = model::offset{100}) {
    iobuf data;
    data.append(ss::temporary_buffer<char>(bytes));
    return read_result(
      std::move(data),
      model::offset{5},
      model::offset{5},
      model::offset{5},
      1,
      hwm,
      lso,
      std::nullopt,
      {});
}

// get_or_insert is the only public entry point, so tests drive it and prove
// coalescing behaviorally. Each read_fn bumps `calls`, so a shared serve leaves
// the count untouched and a miss increments it. read_fn is single-use
// (move-only), so a fresh one is built per get_or_insert call.
fetch_read_coalescer::read_fn counting_read(
  int& calls,
  size_t bytes = 7,
  model::offset hwm = model::offset{100},
  model::offset lso = model::offset{100}) {
    return [&calls, bytes, hwm, lso] {
        ++calls;
        return ss::make_ready_future<read_result>(make_result(bytes, hwm, lso));
    };
}

fetch_read_coalescer::read_fn counting_error(int& calls) {
    return [&calls] {
        ++calls;
        return ss::make_ready_future<read_result>(read_result(
          error_code::offset_out_of_range,
          model::offset{5},
          model::offset{100},
          model::offset{100}));
    };
}

fetch_read_coalescer::read_fn counting_empty(int& calls) {
    return [&calls] {
        ++calls;
        return ss::make_ready_future<read_result>(read_result(
          model::offset{5}, model::offset{100}, model::offset{100}));
    };
}

} // namespace

TEST_CORO(FetchReadCoalescer, RetainAndShare) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    auto sr = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 1);

    // A later reader at the same (fresh) bound shares the retained result
    // verbatim without re-reading — while a strong ref (sr) still holds it.
    auto shared = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(sr.get(), shared.get());

    // Dropping the last strong ref expires the weak cache entry, so the next
    // reader misses and re-reads.
    sr = nullptr;
    shared = nullptr;
    co_await c.get_or_insert(key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 2);
}

TEST_CORO(FetchReadCoalescer, DistinctKeysDoNotCoalesce) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());

    int calls = 0;
    // Hold every result so retention can't lapse; distinct keys must still each
    // drive their own read.
    std::vector<shared_read> held;
    held.push_back(
      co_await c.get_or_insert(
        make_key(), model::offset{100}, counting_read(calls)));
    EXPECT_EQ(calls, 1);

    // Different offset, max_bytes, isolation level, or obligatory flag -> a
    // different key -> a fresh read, never a share.
    held.push_back(
      co_await c.get_or_insert(
        make_key(model::offset{6}), model::offset{100}, counting_read(calls)));
    EXPECT_EQ(calls, 2);
    held.push_back(
      co_await c.get_or_insert(
        make_key(model::offset{5}, 2048),
        model::offset{100},
        counting_read(calls)));
    EXPECT_EQ(calls, 3);
    held.push_back(
      co_await c.get_or_insert(
        make_key(
          model::offset{5}, 1024, model::isolation_level::read_committed),
        model::offset{100},
        counting_read(calls)));
    EXPECT_EQ(calls, 4);
    held.push_back(
      co_await c.get_or_insert(
        make_key(
          model::offset{5},
          1024,
          model::isolation_level::read_uncommitted,
          false),
        model::offset{100},
        counting_read(calls)));
    EXPECT_EQ(calls, 5);
}

TEST_CORO(FetchReadCoalescer, InflightHitSharesOneRead) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    ss::promise<read_result> block;
    // Initiator: its read_fn runs but suspends on `block`, leaving the read in
    // flight.
    auto initiator = c.get_or_insert(key, model::offset{100}, [&calls, &block] {
        ++calls;
        return block.get_future();
    });

    // A concurrent reader awaits the in-flight read rather than starting a
    // second one, so its read_fn never runs.
    auto waiter = c.get_or_insert(
      key, model::offset{100}, counting_read(calls));

    block.set_value(make_result());
    auto sr_initiator = co_await std::move(initiator);
    auto sr_waiter = co_await std::move(waiter);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(sr_initiator.get(), sr_waiter.get());
}

TEST_CORO(FetchReadCoalescer, StaleResultReReads) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    // Read at hwm=100.
    auto sr = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls, 7, model::offset{100}));
    EXPECT_EQ(calls, 1);

    // Same bound is fresh -> shared. A grown bound is stale -> the reader must
    // re-read.
    auto fresh = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(sr.get(), fresh.get());

    auto grown = co_await c.get_or_insert(
      key, model::offset{200}, counting_read(calls, 9, model::offset{200}));
    EXPECT_EQ(calls, 2);
    EXPECT_NE(sr.get(), grown.get());
}

TEST_CORO(FetchReadCoalescer, StaleEntryRegeneratesInPlace) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    auto first = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls, 7, model::offset{100}));
    EXPECT_EQ(calls, 1);

    // Partition grew: the retained result is stale, so the reader re-reads and
    // regenerates the entry in place with the grown result.
    auto second = co_await c.get_or_insert(
      key, model::offset{200}, counting_read(calls, 9, model::offset{200}));
    EXPECT_EQ(calls, 2);
    EXPECT_NE(first.get(), second.get());

    // A reader at the new bound now ready-hits the regenerated result.
    auto third = co_await c.get_or_insert(
      key, model::offset{200}, counting_read(calls));
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(second.get(), third.get());
}

TEST_CORO(FetchReadCoalescer, ErrorResultNotRetained) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    auto sr = co_await c.get_or_insert(
      key, model::offset{100}, counting_error(calls));
    // The error is handed to the initiator (and would be to waiters)...
    EXPECT_EQ(sr->error, error_code::offset_out_of_range);
    EXPECT_EQ(calls, 1);

    // ...but never retained: the next reader re-reads.
    co_await c.get_or_insert(key, model::offset{100}, counting_error(calls));
    EXPECT_EQ(calls, 2);
}

TEST_CORO(FetchReadCoalescer, EmptyResultNotRetained) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    auto sr = co_await c.get_or_insert(
      key, model::offset{100}, counting_empty(calls));
    EXPECT_FALSE(sr->has_data());
    EXPECT_EQ(calls, 1);

    // An empty response drains instantly, so it isn't retained: re-read.
    co_await c.get_or_insert(key, model::offset{100}, counting_empty(calls));
    EXPECT_EQ(calls, 2);
}

TEST_CORO(FetchReadCoalescer, ReadThrowsPropagatesAndDoesNotRetain) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    bool threw = false;
    try {
        co_await c.get_or_insert(key, model::offset{100}, [&calls] {
            ++calls;
            return ss::make_exception_future<read_result>(
              std::runtime_error("boom"));
        });
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_EQ(calls, 1);

    // A throw leaves nothing retained: the next reader re-reads.
    co_await c.get_or_insert(key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 2);
}

TEST_CORO(FetchReadCoalescer, DisableClearsCache) {
    config::mock_property<bool> enabled{true};
    fetch_read_coalescer c(test_cache_config(), enabled.bind());
    auto key = make_key();

    int calls = 0;
    auto sr = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls));
    auto shared = co_await c.get_or_insert(
      key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 1);

    enabled.update(false);
    co_await ss::yield();
    EXPECT_FALSE(c.enabled());

    enabled.update(true);
    co_await ss::yield();
    EXPECT_TRUE(c.enabled());

    // Re-enabling starts from an empty cache: even though sr/shared still hold
    // the blob, the entry is gone, so the reader misses and re-reads.
    co_await c.get_or_insert(key, model::offset{100}, counting_read(calls));
    EXPECT_EQ(calls, 2);
}

} // namespace kafka
