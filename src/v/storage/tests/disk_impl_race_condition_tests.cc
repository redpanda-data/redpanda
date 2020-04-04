#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/core/temporary_buffer.hh>

struct fixture {
    storage::disk_log_builder b{storage::log_config{
      .base_dir = storage::random_dir(),
      .max_segment_size = (1 << 30),
      .should_sanitize = storage::log_config::sanitize_files::yes,
      .compaction_interval = std::chrono::minutes(1),
      .disable_cache = storage::log_config::disable_batch_cache::yes}};
    ~fixture() { b.stop().get(); }
};

FIXTURE_TEST(release_appender_race, fixture) {
    using namespace storage; // NOLINT
    b | start() | add_segment(0)
      | add_random_batch(0, 100, maybe_compress_batches::yes);

    auto& l = b.get_disk_log_impl();
    {
        // new scope to trigger appender destructor

        auto app = l.make_appender(append_config());
        app.initialize().get(); // get the a read-lock

        // NOTE: trigger bug
        l.maybe_roll(
           model::term_id(1), model::offset(100), ss::default_priority_class())
          .get();

        // NOTE: this will crash if the race condition is true
        auto batches = test::make_random_batches(model::offset(100), 1);
        BOOST_REQUIRE_EQUAL(batches.size(), 1);
        for (model::record_batch& b : batches) {
            b.header().base_offset = model::offset(100);
            b.header().ctx.term = model::term_id(1);
            app(std::move(b)).get();
        }
        app.end_of_stream().get();
    }
    info("log: {}", b.get_log());
    l.flush().get();
    auto recs = b.consume().get0();
    BOOST_REQUIRE_EQUAL(recs.size(), 2);
}
