#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/log_segment_reader.h"
#include "storage/tests/utils/log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/testing/thread_test_case.hh>

static ss::logger fixturelog{"log_fixture"};
class log_builder_fixture {
public:
    struct log_stats {
        size_t seg_count{0};
        size_t batch_count{0};
        size_t record_count{0};
    };

    log_builder_fixture()
      : b(make_dir(), make_ntp()) {}

    ss::future<log_stats> get_stats() {
        return b.with_log([](storage::log log) {
            fixturelog.info("log : {}", log);
            auto reader = log.make_reader(storage::log_reader_config(
              model::offset(0),
              model::model_limits<model::offset>::max(),
              ss::default_priority_class()));
            return ss::do_with(
              std::move(reader), [log](model::record_batch_reader& reader) {
                  return reader.consume(stat_consumer(), model::no_timeout)
                    .then([log](log_stats stats) {
                        stats.seg_count = log.segment_count();
                        return ss::make_ready_future<log_stats>(stats);
                    });
              });
        });
    }

    storage::log_builder b;

private:
    struct stat_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
            stats_.batch_count++;
            stats_.record_count += batch.record_count();
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        log_stats end_of_stream() { return stats_; }

    private:
        log_stats stats_;
    };

    static ss::sstring make_dir() {
        return fmt::format(
          "test.dir_{}", random_generators::gen_alphanum_string(7));
    }

    static model::ntp make_ntp() {
        return model::ntp{
          .ns = model::ns("ns"),
          .tp = {.topic = model::topic("topic"),
                 .partition = model::partition_id(0)},
        };
    }
};

FIXTURE_TEST(kitchen_sink, log_builder_fixture) {
    auto extra = storage::test::make_random_batches(model::offset(102), 3);

    auto bspec = storage::log_builder::batch_spec{
      .num_records = 2, .compression = model::compression::none};

    b.segment()                    // start a new segment
      .add_random_batch(100)       // add batch with 100 random record
      .add_random_batch(2)         // add batch with 2 random records
      .segment()                   // start a new segment
      .add_batch(std::move(extra)) // add some external batches
      .segment()                   // start a new segment
      .add_batch(bspec)            // add batch with custom spec
      .flush()
      .get();

    auto stats = get_stats().get0();
    BOOST_TEST(stats.seg_count == 3);
    BOOST_TEST(stats.batch_count == 6);
    BOOST_TEST(stats.record_count > 104);
}
