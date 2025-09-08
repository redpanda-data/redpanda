/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/tests/async_data_uploader_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include <exception>

using namespace archival;

namespace {

constexpr size_t max_upload_size{128_MiB};
constexpr ss::lowres_clock::duration segment_lock_timeout{60s};

struct SegmentCollectorBench : public redpanda_thread_fixture {
    SegmentCollectorBench()
      : redpanda_thread_fixture() {
        wait_for_controller_leadership().get();
    }

    auto get_test_partition() {
        return app.partition_manager.local().get(test_ntp).get();
    }

    template<class GenFunc>
    ss::future<> produce_data(size_t num_batches, GenFunc generator) {
        auto client = co_await redpanda_thread_fixture::make_kafka_client();
        tests::kafka_produce_transport producer(std::move(client));
        co_await producer.start();
        std::exception_ptr eptr{};
        try {
            for (size_t i = 0; i < num_batches; i++) {
                std::vector<tests::kv_t> records = generator();
                auto ts = _produce_timestamp;
                _produce_timestamp = model::timestamp(ts.value() + 1);
                co_await producer.produce_to_partition(
                  test_ntp.tp.topic,
                  test_ntp.tp.partition,
                  std::move(records),
                  ts);
            }
        } catch (...) {
            eptr = std::current_exception();
        }
        co_await producer.stop();
        if (eptr) {
            std::rethrow_exception(eptr);
        }
    }

    ss::future<>
    populate_log(size_t n_segs = 10, size_t batches_per_seg = 100) {
        for (size_t i = 0; i < n_segs; ++i) {
            random_records_generator generator;
            co_await produce_data(batches_per_seg, generator);
            co_await roll_segment();
        }
    }

    storage::disk_log_impl* get_partition_log() {
        auto slog = get_test_partition()->log();
        return dynamic_cast<storage::disk_log_impl*>(slog.get());
    }

    ss::future<> roll_segment() {
        auto log = get_partition_log();
        co_await log->flush();
        co_await log->force_roll();
    }

    ss::future<> setup() {
        static constexpr int topic_name_len = 30;
        auto name = model::topic{
          random_generators::gen_alphanum_string(topic_name_len)};
        test_ntp = model::ntp{
          model::kafka_namespace, name, model::partition_id{0}};
        co_await create_topic();
        co_await populate_log(1, 500);
    }

    segment_collector collect_segments() {
        segment_collector collector{
          segment_collector_mode::new_upload,
          model::offset{0},
          _manifest,
          *get_partition_log(),
          max_upload_size,
          std::nullopt /* end_inclusive */,
          get_test_partition()->last_stable_offset() /* end_exclusive */,
          std::nullopt /* flush_offset */,
        };
        collector.collect_segments();
        return collector;
    }

    ss::future<> create_topic() {
        co_await redpanda_thread_fixture::add_topic(
          model::topic_namespace(test_ntp.ns, test_ntp.tp.topic),
          1,
          std::nullopt);
        co_await redpanda_thread_fixture::wait_for_leader(test_ntp);
    }

    ss::future<> consume_stream(segment_collector_stream_result res) {
        vassert(
          std::holds_alternative<segment_collector_stream>(res),
          "Expected stream but got {}",
          res.index());
        auto strm = std::get<segment_collector_stream>(std::move(res));
        iobuf d;
        auto is = strm.create_input_stream();
        while (!is.eof()) {
            auto buf = co_await is.read();
            if (buf.empty()) {
                break;
            }
            d.append(std::move(buf));
        }
        co_await is.close();
        vassert(
          d.size_bytes() == strm.size,
          "Data size mismatch. stream: {} != data: {}",
          human::bytes(d.size_bytes()),
          human::bytes(strm.size));
    }

    ss::gate gate;
    model::ntp test_ntp;
    model::timestamp _produce_timestamp{0};
    cloud_storage::partition_manifest _manifest{};
};

static constexpr int n_iter_per_run = 100;

ss::future<size_t> do_make_upload_candidate_stream(SegmentCollectorBench* t) {
    co_await t->setup();

    for (int i = 0; i < n_iter_per_run; ++i) {
        auto collector = t->collect_segments();
        vassert(
          collector.segment_ready_for_upload(), "Failed to collect segments");
        perf_tests::start_measuring_time();
        auto res = co_await collector.make_upload_candidate_stream(
          segment_lock_timeout);
        co_await t->consume_stream(std::move(res));
        perf_tests::stop_measuring_time();
    }
    co_return n_iter_per_run;
}

ss::future<size_t> do_make_segment_upload_stream(SegmentCollectorBench* t) {
    co_await t->setup();

    for (int i = 0; i < n_iter_per_run; ++i) {
        auto collector = t->collect_segments();
        vassert(
          collector.segment_ready_for_upload(), "Failed to collect segments");
        perf_tests::start_measuring_time();
        auto res = co_await collector.make_segment_upload_stream(
          *t->get_test_partition(), segment_lock_timeout, t->gate);
        co_await t->consume_stream(std::move(res));
        perf_tests::stop_measuring_time();
    }
    co_return n_iter_per_run;
}

} // namespace

PERF_TEST_F(SegmentCollectorBench, make_upload_cand_stream) {
    return do_make_upload_candidate_stream(this);
}

PERF_TEST_F(SegmentCollectorBench, make_seg_upload_stream) {
    return do_make_segment_upload_stream(this);
}
