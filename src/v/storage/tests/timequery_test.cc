#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>

FIXTURE_TEST(timequery, log_builder_fixture) {
    using namespace storage; // NOLINT

    b | start();

    // seg0: timestamps 0..99, offset = timestamp
    b | add_segment(0);
    for (auto ts = 0; ts < 100; ts++) {
        auto batch = test::make_random_batch(model::offset(ts), 1, false);
        batch.header().first_timestamp = model::timestamp(ts);
        batch.header().max_timestamp = model::timestamp(ts);
        b | add_batch(std::move(batch));
    }

    // seg1: [(offset, ts)..]
    //  - (100, 100), (101, 100), ... (104, 100)
    //  - (105, 101), (105, 101), ... (109, 101)
    b | add_segment(100);
    for (auto offset = 100; offset <= 200; offset++) {
        auto ts = 100 + (offset - 100) / 5;
        auto batch = test::make_random_batch(model::offset(offset), 1, false);
        batch.header().first_timestamp = model::timestamp(ts);
        batch.header().max_timestamp = model::timestamp(ts);
        b | add_batch(std::move(batch));
    }

    // in the first segment check that query(ts) -> batch.offset = ts.
    for (auto ts = 0; ts < 100; ts++) {
        auto log = b.get_log();

        storage::timequery_config config(
          model::timestamp(ts),
          log.offsets().dirty_offset,
          ss::default_priority_class());

        auto res = log.timequery(config).get0();
        BOOST_TEST(res);
        BOOST_TEST(res->time == model::timestamp(ts));
        BOOST_TEST(res->offset == model::offset(ts));
    }

    // in the second segment
    //   query(100) -> batch.offset = 100
    //   query(101) -> batch.offset = 105
    //   query(105) -> batch.offset = 125
    //   query(106) -> batch.offset = 130
    //   ...
    //   query(120) -> batch.offset = 200
    for (auto ts = 100; ts <= 120; ts++) {
        auto log = b.get_log();

        storage::timequery_config config(
          model::timestamp(ts),
          log.offsets().dirty_offset,
          ss::default_priority_class());

        auto offset = (ts - 100) * 5 + 100;

        auto res = log.timequery(config).get0();
        BOOST_TEST(res);
        BOOST_TEST(res->time == model::timestamp(ts));
        BOOST_TEST(res->offset == model::offset(offset));
    }

    b | stop();
}
