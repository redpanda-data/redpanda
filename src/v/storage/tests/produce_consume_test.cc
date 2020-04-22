

#include "model/record_batch_reader.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"

#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

SEASTAR_THREAD_TEST_CASE(produce_consume_concurrency) {
    auto cfg = log_builder_config();
    cfg.cache = storage::log_config::with_cache::no;
    storage::disk_log_builder builder(std::move(cfg));
    builder | storage::start();

    storage::log_append_config app_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    auto& log = builder.get_log();
    auto range = boost::irange(0, 1000);

    auto prod = ss::do_for_each(
      range.begin(), range.end(), [app_cfg, &log](int) {
          auto appender = log.make_appender(app_cfg);
          return ss::do_with(
            model::make_memory_record_batch_reader(
              storage::test::make_random_batches(model::offset(0), 1)),
            [app_cfg, &log](model::record_batch_reader& rdr) {
                return rdr
                  .for_each_ref(log.make_appender(app_cfg), model::no_timeout)
                  .then([&log](auto) { return log.flush(); });
            });
      });

    auto consumer = ss::do_for_each(range.begin(), range.end(), [&log](int) {
        storage::log_reader_config rdr_cfg(
          log.dirty_offset() < model::offset(0)
            ? log.dirty_offset()
            : log.dirty_offset() - model::offset(1),
          std::max(model::offset(0), log.dirty_offset()),
          ss::default_priority_class());
        return log.make_reader(rdr_cfg)
          .then([](model::record_batch_reader reader) {
              return model::consume_reader_to_memory(
                std::move(reader), model::no_timeout);
          })
          .discard_result();
    });

    ss::when_all(std::move(prod), std::move(consumer), [] {}).get0();

    builder | storage::stop();
}
