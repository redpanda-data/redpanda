#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/log_segment_reader.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/testing/thread_test_case.hh>

#include <iostream>

static ss::logger fixturelog{"log_fixture"};
class log_builder_fixture {
public:
    struct log_stats {
        size_t seg_count{0};
        size_t batch_count{0};
        size_t record_count{0};
    };

    log_builder_fixture() = default;

    ss::future<log_stats> get_stats() {
        return b.consume<stat_consumer>().then([this](log_stats stats) {
            stats.seg_count = b.get_log().segment_count();
            return ss::make_ready_future<log_stats>(stats);
        });
    }

    storage::disk_log_builder b;

private:
    struct stat_consumer {
        using ret_type = log_stats;

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
};

FIXTURE_TEST(kitchen_sink, log_builder_fixture) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0) | add_random_batch(0, 100, compression::yes)
      | add_random_batch(100, 2, compression::yes) | add_segment(102)
      | add_random_batch(102, 2, compression::yes) | add_segment(104)
      | add_random_batches(104, 3);

    auto stats = get_stats().get0();

    b | stop();
    BOOST_TEST(stats.seg_count == 3);
    BOOST_TEST(stats.batch_count == 6);
    BOOST_TEST(stats.record_count >= 104);
}
