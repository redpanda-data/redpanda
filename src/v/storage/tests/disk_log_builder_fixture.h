#pragma once
#include "storage/tests/utils/disk_log_builder.h"

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
