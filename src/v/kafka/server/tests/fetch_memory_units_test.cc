/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/configuration.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "kafka/server/fetch_memory_units.h"
#include "ssx/semaphore.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <functional>
#include <optional>

using namespace std::chrono_literals;

namespace {
void set_units(ssx::semaphore& sem, size_t target_units) {
    auto current_units = sem.current();

    if (target_units < current_units) {
        sem.consume(current_units - target_units);
    }
    if (target_units > current_units) {
        sem.signal(target_units - current_units);
    }
}
} // namespace

class fetch_memory_units_test_fixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        co_await _kafka_sem.start(100_MiB, ss::sstring("kafka_sem"));
        co_await _fetch_sem.start(50_MiB, ss::sstring("fetch_sem"));
        co_await _max_message_size.start(std::nullopt);
        co_await _manager.start(
          ss::sharded_parameter(
            [this] { return std::reference_wrapper(_kafka_sem.local()); }),
          ss::sharded_parameter(
            [this] { return std::reference_wrapper(_fetch_sem.local()); }),
          [this] -> kafka::fetch_memory_units_manager& {
              return _manager.local();
          },
          ss::sharded_parameter(
            [this] { return _max_message_size.local().bind(); }));
    }

    ss::future<> TearDownAsync() override {
        co_await _manager.stop();
        co_await _max_message_size.stop();
        co_await _kafka_sem.stop();
        co_await _fetch_sem.stop();
    }

    kafka::fetch_memory_units_manager& local_manager() {
        return _manager.local();
    }

    ss::sharded<kafka::fetch_memory_units_manager>& sharded_manager() {
        return _manager;
    }

    ss::sharded<ssx::semaphore>& sharded_kafka_sem() { return _kafka_sem; }

    ssx::semaphore& local_kafka_semaphore() { return _kafka_sem.local(); }

    ss::sharded<ssx::semaphore>& sharded_fetch_sem() { return _fetch_sem; }

    ssx::semaphore& local_fetch_semaphore() { return _fetch_sem.local(); }

    ss::future<> set_kafka_units(size_t target_units) {
        return _kafka_sem.invoke_on_all(
          [target_units](auto& ks) { set_units(ks, target_units); });
    }

    ss::future<> set_fetch_units(size_t target_units) {
        return _fetch_sem.invoke_on_all(
          [target_units](auto& fs) { set_units(fs, target_units); });
    }

    ss::future<> set_max_message_size(std::optional<int32_t> s) {
        return _max_message_size.invoke_on_all(
          [s](auto& ms) { ms.update(std::optional{s}); });
    }

private:
    ss::sharded<ssx::semaphore> _kafka_sem;
    ss::sharded<ssx::semaphore> _fetch_sem;
    ss::sharded<kafka::fetch_memory_units_manager> _manager;
    ss::sharded<ssx::semaphore> _sem;
    ss::sharded<config::mock_property<std::optional<int32_t>>>
      _max_message_size;
};

TEST_F_CORO(fetch_memory_units_test_fixture, test_cross_shard_free) {
    EXPECT_GE(ss::smp::count, 2);

    const auto max_release_size
      = kafka::fetch_memory_units_manager::max_release_size;
    const auto max_release_period
      = kafka::fetch_memory_units_manager::max_release_period;

    co_await set_kafka_units(max_release_size);
    co_await set_fetch_units(max_release_size);

    auto other_shard_id = (ss::this_shard_id() + 1) % ss::smp::count;

    auto other_kafka_sem_avail = [&] {
        return sharded_kafka_sem().invoke_on(other_shard_id, [](auto& sem_sev) {
            return sem_sev.available_units();
        });
    };
    auto other_fetch_sem_avail = [&] {
        return sharded_fetch_sem().invoke_on(other_shard_id, [](auto& sem_sev) {
            return sem_sev.available_units();
        });
    };
    auto get_remote_units = [&](size_t n) {
        return sharded_manager().invoke_on(other_shard_id, [n](auto& mgr) {
            return std::make_optional(
              mgr.allocate_memory_units(model::ktp{}, n, n, n, false));
        });
    };

    auto remote_units = co_await get_remote_units(max_release_size);
    EXPECT_EQ(remote_units.value().num_units(), max_release_size);
    EXPECT_EQ(local_fetch_semaphore().available_units(), max_release_size);
    EXPECT_EQ(local_kafka_semaphore().available_units(), max_release_size);
    EXPECT_EQ(co_await other_fetch_sem_avail(), 0);
    EXPECT_EQ(co_await other_kafka_sem_avail(), 0);

    remote_units = {};
    // Since the number of units is equal to max_release_size they should be
    // sent back to their originating shard right away.
    EXPECT_EQ(co_await other_fetch_sem_avail(), max_release_size);
    EXPECT_EQ(co_await other_kafka_sem_avail(), max_release_size);

    remote_units = co_await get_remote_units(max_release_size - 1);
    EXPECT_EQ(remote_units.value().num_units(), max_release_size - 1);
    EXPECT_NE(co_await other_fetch_sem_avail(), max_release_size);
    EXPECT_NE(co_await other_kafka_sem_avail(), max_release_size);

    remote_units = {};
    // Since the number of units is below the max_release_size they shouldn't of
    // been sent back to their originating shard yet.
    EXPECT_NE(co_await other_fetch_sem_avail(), max_release_size);
    EXPECT_NE(co_await other_kafka_sem_avail(), max_release_size);

    co_await ss::sleep(2 * max_release_period);
    EXPECT_EQ(co_await other_fetch_sem_avail(), max_release_size);
    EXPECT_EQ(co_await other_kafka_sem_avail(), max_release_size);
}

TEST_F_CORO(fetch_memory_units_test_fixture, test_max_units) {
    kafka::fetch_memory_units_manager& mgr = local_manager();

    co_await set_kafka_units(1000);
    co_await set_fetch_units(1000);
    co_await set_max_message_size(10);

    auto units = mgr.allocate_memory_units(model::ktp{}, 1, 100, 1, false);
    EXPECT_EQ(units.num_units(), 10);
    units = mgr.allocate_memory_units(model::ktp{}, 1, 100, 1, true);
    EXPECT_EQ(units.num_units(), 10);

    // `max_bytes` should still be reserved if there are enough units.
    units = mgr.allocate_memory_units(model::ktp{}, 100, 1, 1, false);
    EXPECT_EQ(units.num_units(), 100);
}

TEST_F_CORO(fetch_memory_units_test_fixture, test_adjust_units) {
    kafka::fetch_memory_units_manager& mgr = local_manager();

    co_await set_kafka_units(10);
    co_await set_fetch_units(10);

    auto units = mgr.allocate_memory_units(model::ktp{}, 10, 10, 10, false);
    EXPECT_EQ(units.num_units(), 10);
    units.adjust_units(5);
    EXPECT_EQ(units.num_units(), 5);
    units.adjust_units(10);
    EXPECT_EQ(units.num_units(), 10);
}

TEST_F_CORO(fetch_memory_units_test_fixture, test_allocate_memory_units) {
    static constexpr size_t batch_size = 1_MiB;

    kafka::fetch_memory_units_manager& mgr = local_manager();

    co_await set_kafka_units(100_MiB);
    co_await set_fetch_units(50_MiB);

    const auto test_case =
      [&mgr](size_t max_bytes, bool obligatory_batch_read) -> size_t {
        auto mu = mgr.allocate_memory_units(
          model::ktp{},
          max_bytes,
          batch_size,
          batch_size,
          obligatory_batch_read);
        return mu.num_units();
    };

    // below are test prerequisites, tests are done based on these assumptions
    // if these are not valid, the test needs a change
    size_t kafka_mem = local_kafka_semaphore().available_units();
    size_t fetch_mem = local_fetch_semaphore().available_units();
    EXPECT_TRUE(fetch_mem > batch_size * 3);
    EXPECT_TRUE(kafka_mem > fetch_mem);
    EXPECT_TRUE(batch_size > 100);

    // *** plenty of memory cases
    // kafka_mem > fetch_mem > batch_size
    // Reserved memory is limited by the fetch memory semaphore
    EXPECT_EQ(test_case(batch_size / 100, false), batch_size);
    EXPECT_EQ(test_case(batch_size / 100, true), batch_size);
    EXPECT_EQ(test_case(batch_size, false), batch_size);
    EXPECT_EQ(test_case(batch_size, true), batch_size);
    EXPECT_EQ(test_case(batch_size * 3, false), batch_size * 3);
    EXPECT_EQ(test_case(batch_size * 3, true), batch_size * 3);
    EXPECT_EQ(test_case(fetch_mem, false), fetch_mem);
    EXPECT_EQ(test_case(fetch_mem, true), fetch_mem);
    EXPECT_EQ(test_case(fetch_mem + 1, false), fetch_mem);
    EXPECT_EQ(test_case(fetch_mem + 1, true), fetch_mem);
    EXPECT_EQ(test_case(kafka_mem, false), fetch_mem);
    EXPECT_EQ(test_case(kafka_mem, true), fetch_mem);

    // *** still a lot of mem but kafka mem somewhat used:
    // fetch_mem > kafka_mem > batch_size (fetch_mem - kafka_mem < batch_size)
    // Obligatory reads to not come into play yet because we still have more
    // memory than a single batch, but the amount of memory reserved is limited
    // by the smaller semaphore, which is kafka_mem in this case
    auto memsemunits = ss::consume_units(
      local_kafka_semaphore(), kafka_mem - fetch_mem + 1000);
    kafka_mem = local_kafka_semaphore().available_units();
    EXPECT_TRUE(kafka_mem < fetch_mem);
    EXPECT_TRUE(kafka_mem > batch_size + 1000);

    EXPECT_EQ(test_case(batch_size, false), batch_size);
    EXPECT_EQ(test_case(batch_size, true), batch_size);
    EXPECT_EQ(test_case(kafka_mem - 100, false), kafka_mem - 100);
    EXPECT_EQ(test_case(kafka_mem - 100, true), kafka_mem - 100);
    EXPECT_EQ(test_case(kafka_mem + 100, false), kafka_mem);
    EXPECT_EQ(test_case(kafka_mem + 100, true), kafka_mem);
    EXPECT_EQ(test_case(fetch_mem + 100, false), kafka_mem);
    EXPECT_EQ(test_case(fetch_mem + 100, true), kafka_mem);

    memsemunits.return_all();
    kafka_mem = local_kafka_semaphore().available_units();

    // *** low on fetch memory tests
    // kafka_mem > batch_size > fetch_mem
    // Under this condition, unless obligatory_batch_read, we cannot reserve
    // memory as it's not enough for at least a single batch.
    // If obligatory_batch_read, the reserved amount will always be a single
    // batch.
    memsemunits = ss::consume_units(
      local_fetch_semaphore(), fetch_mem - batch_size + 1000);
    fetch_mem = local_fetch_semaphore().available_units();
    EXPECT_TRUE(kafka_mem > batch_size);
    EXPECT_TRUE(fetch_mem < batch_size);

    EXPECT_EQ(test_case(fetch_mem - 100, false), 0);
    EXPECT_EQ(test_case(fetch_mem - 100, true), batch_size);
    EXPECT_EQ(test_case(batch_size - 100, false), 0);
    EXPECT_EQ(test_case(batch_size - 100, true), batch_size);
    EXPECT_EQ(test_case(kafka_mem - 100, false), 0);
    EXPECT_EQ(test_case(kafka_mem - 100, true), batch_size);
    EXPECT_EQ(test_case(kafka_mem + 100, false), 0);
    EXPECT_EQ(test_case(kafka_mem + 100, true), batch_size);

    memsemunits.return_all();
    fetch_mem = local_fetch_semaphore().available_units();

    // *** low on kafka memory tests
    // fetch_mem > batch_size > kafka_mem
    // Essentially the same behaviour as in low fetch memory cases
    memsemunits = ss::consume_units(
      local_kafka_semaphore(), kafka_mem - batch_size + 1000);
    kafka_mem = local_kafka_semaphore().available_units();
    EXPECT_TRUE(kafka_mem < batch_size);
    EXPECT_TRUE(fetch_mem > batch_size);

    EXPECT_EQ(test_case(kafka_mem - 100, false), 0);
    EXPECT_EQ(test_case(kafka_mem - 100, true), batch_size);
    EXPECT_EQ(test_case(batch_size - 100, false), 0);
    EXPECT_EQ(test_case(batch_size - 100, true), batch_size);
    EXPECT_EQ(test_case(batch_size + 100, false), 0);
    EXPECT_EQ(test_case(batch_size + 100, true), batch_size);
    EXPECT_EQ(test_case(fetch_mem - 100, false), 0);
    EXPECT_EQ(test_case(fetch_mem - 100, true), batch_size);
    EXPECT_EQ(test_case(fetch_mem + 100, false), 0);
    EXPECT_EQ(test_case(fetch_mem + 100, true), batch_size);

    memsemunits.return_all();
    kafka_mem = local_kafka_semaphore().available_units();
    EXPECT_EQ(kafka_mem, 100_MiB);
}
