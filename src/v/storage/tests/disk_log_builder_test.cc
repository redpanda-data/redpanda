#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

FIXTURE_TEST(kitchen_sink, log_builder_fixture) {
    using namespace storage; // NOLINT

    auto batch = test::make_random_batch(model::offset(107), 1, false);

    b | start() | add_segment(0)
      | add_random_batch(0, 100, maybe_compress_batches::yes)
      | add_random_batch(100, 2, maybe_compress_batches::yes) | add_segment(102)
      | add_random_batch(102, 2, maybe_compress_batches::yes) | add_segment(104)
      | add_random_batches(104, 3) | add_batch(std::move(batch));

    auto stats = get_stats().get0();

    b | stop();
    BOOST_TEST(stats.seg_count == 3);
    BOOST_TEST(stats.batch_count == 7);
    BOOST_TEST(stats.record_count >= 105);
}
