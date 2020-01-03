#include "storage2/common.h"
#include "storage2/detail/index.h"
#include "storage2/repository.h"
#include "storage2/segment_index.h"
#include "storage2/tests/random_batch.h"
#include "storage2/tests/test_common.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <boost/core/typeinfo.hpp>
#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/format.h>

using namespace storage;       // NOLINT
using namespace storage::test; // NOLINT

static model::ntp
make_ntp(std::string ns, std::string topic, size_t partition_id) {
    return model::ntp{.ns = model::ns(std::move(ns)),
                      .tp = {.topic = model::topic(std::move(topic)),
                             .partition = model::partition_id(partition_id)}};
}

SEASTAR_THREAD_TEST_CASE(test_write_few_small_batches) {
    configure_unit_test_logging();
    const auto config = repository::config::testing_defaults();
    repository repo = repository::open(make_test_dir(), config).get0();
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();
    auto ar1 = t1p0.append(test::make_random_batch_v2(100)).get0();
    auto ar2 = t1p0.append(test::make_random_batch_v2(200)).get0();
    auto ar3 = t1p0.append(test::make_random_batch_v2(300)).get0();

    // auto istream = t1p0.read(model::offset(30));
    // istream.close().wait();
    t1p0.close().wait();

    // BOOST_TEST_REQUIRE assigned offsets to be correct.
}

SEASTAR_THREAD_TEST_CASE(test_write_empty_batch) {
    configure_unit_test_logging();
    const auto config = repository::config::testing_defaults();
    repository repo = repository::open(make_test_dir(), config).get0();
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();

    BOOST_CHECK_THROW(
      t1p0.append(make_random_batch_v2(0)).get0(), storage::record_batch_error);

    t1p0.close().wait();
}

SEASTAR_THREAD_TEST_CASE(
  test_write_many_batches_progressive_multiple_segments) {
    configure_unit_test_logging();
    storage::repository::config config{
      .max_segment_size = 10_mb,
      .should_sanitize = storage::repository::config::sanitize_files::no,
      .enable_lazy_loading = storage::repository::config::lazy_loading::no,
      .io_priority = default_priority_class()};

    repository repo
      = repository::open(make_test_dir(), std::move(config)).get0();
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();

    std::vector<storage::append_result> results;
    for (auto i = 1; i <= 10; ++i) {
        results.emplace_back(
          t1p0.append(test::make_random_batch_v2(i * 100)).get0());
    }

    // BOOST_TEST_REQUIRE assigned offsets to be correct.
    t1p0.close().wait();
}