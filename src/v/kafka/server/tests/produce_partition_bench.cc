/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "container/chunked_vector.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/server/handlers/produce.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/sstring.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/test/unit_test_log.hpp>

using namespace std::chrono_literals; // NOLINT

ss::logger plog("produce_partition_bench");

namespace {
enum class measured_region {
    submitted,
    dispatched,
    produced,
};
} // namespace

struct produce_partition_fixture : redpanda_thread_fixture {
    static constexpr size_t topic_name_length = 30;
    static constexpr size_t total_partition_count = 1;

    model::topic t;

    produce_partition_fixture() {
        BOOST_TEST_CHECKPOINT("before leadership");

        wait_for_controller_leadership().get();

        BOOST_TEST_CHECKPOINT("HERE");

        t = model::topic(
          random_generators::gen_alphanum_string(topic_name_length));
        auto tp = model::topic_partition(t, model::partition_id(0));
        add_topic(
          model::topic_namespace_view(model::kafka_namespace, t),
          total_partition_count)
          .get();

        auto ntp = make_default_ntp(tp.topic, tp.partition);
        wait_for_leader(ntp).get();
        BOOST_TEST_CHECKPOINT("HERE");
    }
    ss::future<> run_test(size_t data_size, measured_region region);
};

ss::future<>
produce_partition_fixture::run_test(size_t data_size, measured_region region) {
    BOOST_TEST_CHECKPOINT("HERE");

    model::topic_partition tp = model::topic_partition(
      t, model::partition_id(0));

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});

    constexpr size_t num_records = 100;
    for (size_t i = 0; i < num_records; ++i) {
        builder.add_raw_kv(iobuf{}, rand_iobuf(data_size));
    }

    auto batch = std::move(builder).build();

    chunked_vector<kafka::produce_request::partition> partitions;
    partitions.push_back(kafka::produce_request::partition{
      .partition_index{model::partition_id(0)},
      .records = kafka::produce_request_record_data(std::move(batch))});

    chunked_vector<kafka::produce_request::topic> topics;
    topics.push_back(kafka::produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});

    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    kafka::produce_request produce_req = kafka::produce_request(
      t_id, acks, std::move(topics));

    auto conn = make_connection_context();

    BOOST_TEST_CHECKPOINT("HERE");

    kafka::request_header header{
      .key = kafka::produce_handler::api::key,
      .version = kafka::produce_handler::max_supported};

    // Use a fake_req to generate the context because encoding the request
    // steals the request's iobufs.
    kafka::produce_request fake_req;
    auto rctx = make_request_context(std::move(fake_req), header, conn);

    BOOST_TEST_CHECKPOINT("HERE");

    kafka::produce_ctx pctx{
      std::move(rctx),
      std::move(produce_req),
      kafka::produce_response{},
      ss::default_smp_service_group()};

    auto& topic = pctx.request.data.topics.front();
    auto& partition = topic.partitions.front();

    // Drain task queue before running the measured region in order to
    // reduce noise from unrelated tasks.
    co_await tests::drain_task_queue();

    perf_tests::start_measuring_time();
    auto stages = kafka::testing::produce_single_partition(
      pctx, topic, partition);

    if (region == measured_region::submitted) {
        perf_tests::stop_measuring_time();
    }

    auto fut = co_await ss::coroutine::as_future(std::move(stages.dispatched));
    if (fut.failed()) {
        vlog(
          plog.error,
          "unable to dispatch produce request: {}",
          fut.get_exception());
        co_return;
    }

    if (region == measured_region::dispatched) {
        perf_tests::stop_measuring_time();
    }

    auto produced_fut = co_await ss::coroutine::as_future(
      std::move(stages.produced));
    if (produced_fut.failed()) {
        vlog(
          plog.error,
          "unable to produce records: {}",
          produced_fut.get_exception());
        co_return;
    }

    if (region == measured_region::produced) {
        perf_tests::stop_measuring_time();
    }

    auto p = produced_fut.get();
    vassert(
      p.error_code == kafka::error_code::none, "error_code: {}", p.error_code);
}

PERF_TEST_C(produce_partition_fixture, 1_submitted) {
    co_return co_await this->run_test(1, measured_region::submitted);
}
PERF_TEST_C(produce_partition_fixture, 1_KiB_submitted) {
    co_return co_await this->run_test(1024, measured_region::submitted);
}
PERF_TEST_C(produce_partition_fixture, 4_KiB_submitted) {
    co_return co_await this->run_test(4_KiB, measured_region::submitted);
}
PERF_TEST_C(produce_partition_fixture, 8_KiB_submitted) {
    co_return co_await this->run_test(8_KiB, measured_region::submitted);
}

PERF_TEST_C(produce_partition_fixture, 1_dispatched) {
    co_return co_await this->run_test(1, measured_region::dispatched);
}
PERF_TEST_C(produce_partition_fixture, 1_KiB_dispatched) {
    co_return co_await this->run_test(1024, measured_region::dispatched);
}
PERF_TEST_C(produce_partition_fixture, 4_KiB_dispatched) {
    co_return co_await this->run_test(4_KiB, measured_region::dispatched);
}
PERF_TEST_C(produce_partition_fixture, 8_KiB_dispatched) {
    co_return co_await this->run_test(8_KiB, measured_region::dispatched);
}

PERF_TEST_C(produce_partition_fixture, 1_produced) {
    co_return co_await this->run_test(1, measured_region::produced);
}
PERF_TEST_C(produce_partition_fixture, 1_KiB_produced) {
    co_return co_await this->run_test(1024, measured_region::produced);
}
PERF_TEST_C(produce_partition_fixture, 4_KiB_produced) {
    co_return co_await this->run_test(4_KiB, measured_region::produced);
}
PERF_TEST_C(produce_partition_fixture, 8_KiB_produced) {
    co_return co_await this->run_test(8_KiB, measured_region::produced);
}
