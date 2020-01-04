#include "storage2/common.h"
#include "storage2/detail/index.h"
#include "storage2/repository.h"
#include "storage2/segment_index.h"
#include "storage2/tests/random_batch.h"
#include "storage2/tests/storage_test_fixture.h"

using namespace storage;       // NOLINT
using namespace storage::test; // NOLINT

FIXTURE_TEST(test_write_few_small_batches, storage_test_fixture) {
    storage::repository repo = make_repo();
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();
    auto ar1 = t1p0.append(test::make_random_batch_v2(100)).get0();
    auto ar2 = t1p0.append(test::make_random_batch_v2(200)).get0();
    auto ar3 = t1p0.append(test::make_random_batch_v2(300)).get0();

    // auto istream = t1p0.read(model::offset(30));
    // istream.close().wait();
    t1p0.close().wait();

    // BOOST_TEST_REQUIRE assigned offsets to be correct.
};

FIXTURE_TEST(test_write_empty_batch, storage_test_fixture) {
    storage::repository repo = make_repo();
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();

    BOOST_CHECK_THROW(
      t1p0.append(make_random_batch_v2(0)).get0(), storage::record_batch_error);

    t1p0.close().wait();
};

FIXTURE_TEST(
  test_write_many_batches_progressive_multiple_segments, storage_test_fixture) {
    configure_unit_test_logging();
    storage::repository::config config{
      .max_segment_size = 10_mb,
      .should_sanitize = storage::repository::config::sanitize_files::no,
      .enable_lazy_loading = storage::repository::config::lazy_loading::no,
      .io_priority = default_priority_class()};
    storage::repository repo = make_repo(config);
    partition t1p0 = repo.create_ntp(make_ntp("default", "topic-1", 0)).get0();

    std::vector<storage::append_result> results;
    for (auto i = 1; i <= 10; ++i) {
        results.emplace_back(
          t1p0.append(test::make_random_batch_v2(i * 100)).get0());
    }

    // BOOST_TEST_REQUIRE assigned offsets to be correct.
    t1p0.close().wait();
};