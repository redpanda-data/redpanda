// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "kafka/server/handlers/fetch.h"
#include "ssx/semaphore.h"

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>

struct reserve_mem_units_test_result {
    size_t kafka, fetch;
    explicit reserve_mem_units_test_result(size_t size)
      : kafka(size)
      , fetch(size) {}
    reserve_mem_units_test_result(size_t kafka_, size_t fetch_)
      : kafka(kafka_)
      , fetch(fetch_) {}
    friend bool operator==(
      const reserve_mem_units_test_result&,
      const reserve_mem_units_test_result&)
      = default;
    friend std::ostream&
    operator<<(std::ostream& s, const reserve_mem_units_test_result& v) {
        return s << "{kafka: " << v.kafka << ", fetch: " << v.fetch << "}";
    }
};

BOOST_AUTO_TEST_CASE(reserve_memory_units_test) {
    using namespace kafka;
    using namespace std::chrono_literals;
    using r = reserve_mem_units_test_result;

    // reserve memory units, return how many memory units have been reserved
    // from each memory semaphore
    ssx::semaphore memory_sem{100_MiB, "test_memory_sem"};
    ssx::semaphore memory_fetch_sem{50_MiB, "test_memory_fetch_sem"};
    const auto test_case =
      [&memory_sem, &memory_fetch_sem](
        size_t max_bytes,
        bool obligatory_batch_read) -> reserve_mem_units_test_result {
        auto mu = kafka::testing::reserve_memory_units(
          memory_sem, memory_fetch_sem, max_bytes, obligatory_batch_read);
        return {mu.kafka.count(), mu.fetch.count()};
    };

    static constexpr size_t batch_size = 1_MiB;

    // below are test prerequisites, tests are done based on these assumptions
    // if these are not valid, the test needs a change
    size_t kafka_mem = memory_sem.available_units();
    size_t fetch_mem = memory_fetch_sem.available_units();
    BOOST_TEST(fetch_mem > batch_size * 3);
    BOOST_TEST_REQUIRE(kafka_mem > fetch_mem);
    BOOST_TEST_REQUIRE(batch_size > 100);

    // *** plenty of memory cases
    // kafka_mem > fetch_mem > batch_size
    // Reserved memory is limited by the fetch memory semaphore
    BOOST_TEST(test_case(batch_size / 100, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size / 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size * 3, false) == r(batch_size * 3));
    BOOST_TEST(test_case(batch_size * 3, true) == r(batch_size * 3));
    BOOST_TEST(test_case(fetch_mem, false) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem, true) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem + 1, false) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem + 1, true) == r(fetch_mem));
    BOOST_TEST(test_case(kafka_mem, false) == r(fetch_mem));
    BOOST_TEST(test_case(kafka_mem, true) == r(fetch_mem));

    // *** still a lot of mem but kafka mem somewhat used:
    // fetch_mem > kafka_mem > batch_size (fetch_mem - kafka_mem < batch_size)
    // Obligatory reads to not come into play yet because we still have more
    // memory than a single batch, but the amount of memory reserved is limited
    // by the smaller semaphore, which is kafka_mem in this case
    auto memsemunits = ss::consume_units(
      memory_sem, kafka_mem - fetch_mem + 1000);
    kafka_mem = memory_sem.available_units();
    BOOST_TEST_REQUIRE(kafka_mem < fetch_mem);
    BOOST_TEST_REQUIRE(kafka_mem > batch_size + 1000);

    BOOST_TEST(test_case(batch_size, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size, true) == r(batch_size));
    BOOST_TEST(test_case(kafka_mem - 100, false) == r(kafka_mem - 100));
    BOOST_TEST(test_case(kafka_mem - 100, true) == r(kafka_mem - 100));
    BOOST_TEST(test_case(kafka_mem + 100, false) == r(kafka_mem));
    BOOST_TEST(test_case(kafka_mem + 100, true) == r(kafka_mem));
    BOOST_TEST(test_case(fetch_mem + 100, false) == r(kafka_mem));
    BOOST_TEST(test_case(fetch_mem + 100, true) == r(kafka_mem));

    memsemunits.return_all();
    kafka_mem = memory_sem.available_units();

    // *** low on fetch memory tests
    // kafka_mem > batch_size > fetch_mem
    // Under this condition, unless obligatory_batch_read, we cannot reserve
    // memory as it's not enough for at least a single batch.
    // If obligatory_batch_read, the reserved amount will always be a single
    // batch.
    memsemunits = ss::consume_units(
      memory_fetch_sem, fetch_mem - batch_size + 1000);
    fetch_mem = memory_fetch_sem.available_units();
    BOOST_TEST_REQUIRE(kafka_mem > batch_size);
    BOOST_TEST_REQUIRE(fetch_mem < batch_size);

    BOOST_TEST(test_case(fetch_mem - 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size - 100, false) == r(0));
    BOOST_TEST(test_case(batch_size - 100, true) == r(batch_size));
    BOOST_TEST(test_case(kafka_mem - 100, false) == r(0));
    BOOST_TEST(test_case(kafka_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(kafka_mem + 100, false) == r(0));
    BOOST_TEST(test_case(kafka_mem + 100, true) == r(batch_size));

    memsemunits.return_all();
    fetch_mem = memory_fetch_sem.available_units();

    // *** low on kafka memory tests
    // fetch_mem > batch_size > kafka_mem
    // Essentially the same behaviour as in low fetch memory cases
    memsemunits = ss::consume_units(memory_sem, kafka_mem - batch_size + 1000);
    kafka_mem = memory_sem.available_units();
    BOOST_TEST_REQUIRE(kafka_mem < batch_size);
    BOOST_TEST_REQUIRE(fetch_mem > batch_size);

    BOOST_TEST(test_case(kafka_mem - 100, false) == r(0));
    BOOST_TEST(test_case(kafka_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size - 100, false) == r(0));
    BOOST_TEST(test_case(batch_size - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size + 100, false) == r(0));
    BOOST_TEST(test_case(batch_size + 100, true) == r(batch_size));
    BOOST_TEST(test_case(fetch_mem - 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(fetch_mem + 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem + 100, true) == r(batch_size));

    memsemunits.return_all();
    kafka_mem = memory_sem.available_units();
}
