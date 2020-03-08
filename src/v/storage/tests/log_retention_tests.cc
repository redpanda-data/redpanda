#include "storage/tests/utils/disk_log_builder.h"
// fixture
#include "test_utils/fixture.h"

struct gc_fixture {
    storage::disk_log_builder builder;
};

FIXTURE_TEST(empty_log_garbage_collect, gc_fixture) {
    builder | storage::start()
      | storage::garbage_collect(model::timestamp::now(), std::nullopt)
      | storage::stop();
}

FIXTURE_TEST(garbage_collect_all_segments, gc_fixture) {
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::compression::yes)
      | storage::add_random_batch(100, 2, storage::compression::yes)
      | storage::add_segment(102)
      | storage::add_random_batch(102, 2, storage::compression::yes)
      | storage::add_segment(104) | storage::add_random_batches(104, 3);

    // should *not* collect
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);
    builder | storage::garbage_collect(model::timestamp(1), std::nullopt);
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    builder | storage::garbage_collect(model::timestamp::now(), std::nullopt)
      | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
}
