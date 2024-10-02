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

#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/topics_frontend.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/protocol/produce.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/request_context.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <tuple>

struct fetch_bench_config {
    int num_fetches;

    // Topic settings
    size_t topic_name_length;

    // Message settings
    size_t batch_size;
    size_t batch_overhead;
};

// This config makes data and ktp copying more noticeable in the
// benchmark results.
static constexpr auto large_fetch_config = fetch_bench_config{
  .num_fetches = 100,
  .topic_name_length = 128,
  .batch_size = 500ul * 1024,
  .batch_overhead = 72,
};

static constexpr auto small_fetch_config = fetch_bench_config{
  .num_fetches = 100,
  .topic_name_length = 30,
  .batch_size = 1024,
  .batch_overhead = 70,
};

struct fetch_part {
    model::partition_id pid;
    model::offset offset;
    size_t num_batches;
};

struct fetch_topic {
    model::topic name;
    std::vector<fetch_part> partitions;
};

using fetch_request_config = std::vector<fetch_topic>;

template<fetch_bench_config cfg>
struct fetch_bench_fixture : redpanda_thread_fixture {
    ss::future<size_t> fetch_from(fetch_request_config req_config) {
        std::optional<size_t> total_batches_per_fetch = std::nullopt;
        auto conn_context = make_connection_context();
        co_await conn_context->start();

        auto make_rctx = [&] {
            size_t total_batches = 0;

            chunked_vector<kafka::fetch_topic> fetch_topics;
            for (auto& topic_fetch : req_config) {
                kafka::fetch_topic ft;
                ft.name = topic_fetch.name;

                // add the partitions to the fetch request
                for (const auto& part : topic_fetch.partitions) {
                    kafka::fetch_partition fp;
                    fp.partition_index = model::partition_id(part.pid);
                    fp.fetch_offset = part.offset;
                    fp.current_leader_epoch = kafka::leader_epoch(-1);
                    fp.log_start_offset = model::offset(-1);
                    fp.max_bytes = part.num_batches
                                   * (cfg.batch_size + cfg.batch_overhead);
                    ft.fetch_partitions.push_back(std::move(fp));
                    total_batches += part.num_batches;
                }

                fetch_topics.push_back(std::move(ft));
            }

            if (!total_batches_per_fetch) {
                total_batches_per_fetch = total_batches;
            }

            // create a request
            kafka::fetch_request_data frq_data;
            frq_data.replica_id = kafka::client::consumer_replica_id;
            frq_data.max_wait_ms = 500ms;
            frq_data.min_bytes = total_batches
                                 * (cfg.batch_size + cfg.batch_overhead);
            frq_data.max_bytes = 52428800;
            frq_data.isolation_level = model::isolation_level::read_uncommitted;
            frq_data.session_id = kafka::invalid_fetch_session_id;
            frq_data.session_epoch = kafka::final_fetch_session_epoch;
            frq_data.topics = std::move(fetch_topics);

            auto request = kafka::fetch_request{.data = std::move(frq_data)};

            kafka::request_header header{
              .key = kafka::fetch_handler::api::key,
              .version = kafka::fetch_handler::max_supported};

            return make_request_context(
              std::move(request), header, conn_context);
        };

        std::vector<std::unique_ptr<kafka::op_context>> octxs;
        for (int i = 0; i < cfg.num_fetches + 1; i++) {
            octxs.emplace_back(std::make_unique<kafka::op_context>(
              make_rctx(), ss::default_smp_service_group()));
        }

        // Do a single fetch outside of the measured region first to ensure the
        // fetched batches are in the batch cache in the subsequent fetches.
        co_await kafka::testing::do_fetch(*octxs[cfg.num_fetches]);

        // Drain task queue before running the measured region in order to
        // reduce noise from unrelated tasks.
        co_await tests::drain_task_queue();

        perf_tests::start_measuring_time();
        for (int i = 0; i < cfg.num_fetches; i++) {
            co_await kafka::testing::do_fetch(*octxs[i]);
        }
        perf_tests::stop_measuring_time();

        auto expected_response_size = total_batches_per_fetch.value_or(0)
                                      * (cfg.batch_size + cfg.batch_overhead);
        for (int i = 0; i < cfg.num_fetches + 1; i++) {
            auto& octx = *octxs[i];
            vassert(!octx.response_error, "fetch wasn't successful");
            vassert(
              octx.response_size == expected_response_size,
              "not the expected response size. expected {} actual {}",
              expected_response_size,
              octx.response_size);
        }

        co_return cfg.num_fetches;
    }

    ss::future<> produce_to_topic(
      model::topic t,
      size_t total_partition_count,
      size_t batches_per_partition) {
        auto client = co_await make_kafka_client();
        tests::kafka_produce_transport producer(std::move(client));
        co_await producer.start();
        for (int pid = 0; pid < total_partition_count; pid++) {
            for (int i = 0; i < batches_per_partition; i++) {
                tests::kv_t msg{
                  "", random_generators::gen_alphanum_string(cfg.batch_size)};
                co_await producer.produce_to_partition(
                  t, model::partition_id(pid), {std::move(msg)});
            }
        }
    }

    ss::future<model::topic>
    create_topic(std::vector<model::broker_shard> partitions) {
        auto total_partition_count = partitions.size();
        auto t = model::topic(
          random_generators::gen_alphanum_string(cfg.topic_name_length));

        co_await add_topic(
          model::topic_namespace_view(model::kafka_namespace, t),
          total_partition_count);

        auto ntp_for_pid = [&](auto pid) {
            return model::ntp(
              model::kafka_namespace,
              model::topic_partition(t, model::partition_id(pid)));
        };

        for (auto i = 0; i < total_partition_count; i++) {
            co_await wait_for_leader(ntp_for_pid(i));
        }

        cluster::topics_frontend& tf
          = app.controller->get_topics_frontend().local();
        for (uint32_t pid = 0; pid < partitions.size(); pid++) {
            auto ec = co_await tf.move_partition_replicas(
              ntp_for_pid(pid),
              {partitions[pid]},
              cluster::reconfiguration_policy::full_local_retention,
              model::timeout_clock::now() + 30s);

            vassert(ec == cluster::errc::success, "failed to move partition");
        }

        auto& topic_table = app.controller->get_topics_state().local();

        // There is only one node in the cluster(the one this test runs in).
        // Hence its topic_table should immediately know about all
        // reconfigurations. Therefore waiting until there is no on-going
        // updates will let us know if the previous partition movements have
        // finished.
        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&topic_table] {
            return !topic_table.has_updates_in_progress();
        });

        for (uint32_t pid = 0; pid < partitions.size(); pid++) {
            auto part_info = topic_table.get_partition_assignment(
              ntp_for_pid(pid));
            vassert(part_info.has_value(), "partition doesn't exist");

            auto assign = part_info.value();
            vassert(assign.replicas.size() == 1, "there should be one replica");
            vassert(
              assign.replicas[0] == partitions[pid], "reassignment failed");
        }

        co_return t;
    }

    fetch_bench_fixture() {
        wait_for_controller_leadership().get();

        // Disable as many background processes as possible to reduce noise.
        test_local_cfg.get("log_disable_housekeeping_for_tests")
          .set_value(true);
        test_local_cfg.get("disable_cluster_recovery_loop_for_tests")
          .set_value(true);
        test_local_cfg.get("retention_local_strict").set_value(true);
        test_local_cfg.get("enable_metrics_reporter").set_value(false);

        test_local_cfg.get("fetch_read_strategy")
          .set_value(model::fetch_read_strategy::non_polling);

        restart();
        wait_for_controller_leadership().get();
    }

    // Creates a topic with a single partition that is on shard 0.
    ss::future<model::topic> initialize_single_partition_topic() {
        auto t = co_await create_topic(
          {model::broker_shard{model::node_id{0}, 0}});
        co_await produce_to_topic(t, 1, 1);
        co_return t;
    }

    // Creates a topic with two partitions. One on shard 0 the other on shard 1.
    ss::future<model::topic> initialize_multi_partition_topic() {
        vassert(ss::smp::count >= 2, "requires at least 2 shards");

        auto t = co_await create_topic({
          model::broker_shard{model::node_id{0}, 0},
          model::broker_shard{model::node_id{0}, 1},
        });
        co_await produce_to_topic(t, 2, 1);
        co_return t;
    }

    scoped_config test_local_cfg;
};

using large_fetch_t = fetch_bench_fixture<large_fetch_config>;
using small_fetch_t = fetch_bench_fixture<small_fetch_config>;

fetch_request_config single_partition_req_config(model::topic t) {
    return fetch_request_config{fetch_topic{
      .name = std::move(t),
      .partitions = {fetch_part{model::partition_id(0), model::offset(0), 1}}}};
}

fetch_request_config multi_partition_req_config(model::topic t) {
    return fetch_request_config{fetch_topic{
      .name = std::move(t),
      .partitions = {
        fetch_part{model::partition_id(0), model::offset(0), 1},
        fetch_part{model::partition_id(1), model::offset(0), 1}}}};
}

PERF_TEST_CN(large_fetch_t, single_partition_fetch) {
    static model::topic t = co_await initialize_single_partition_topic();

    co_return co_await fetch_from(single_partition_req_config(t));
}

PERF_TEST_CN(small_fetch_t, single_partition_fetch) {
    static model::topic t = co_await initialize_single_partition_topic();

    co_return co_await fetch_from(single_partition_req_config(t));
}

PERF_TEST_CN(large_fetch_t, multi_partition_fetch) {
    static model::topic t = co_await initialize_multi_partition_topic();

    // One partition will be from the same shard, while the other will be
    // from a foreign shard. This will hopefully allow us to detect if more
    // or less cross shard calls are being made for a fetch.
    co_return co_await fetch_from(multi_partition_req_config(t));
}

PERF_TEST_CN(small_fetch_t, multi_partition_fetch) {
    static model::topic t = co_await initialize_multi_partition_topic();

    // One partition will be from the same shard, while the other will be
    // from a foreign shard. This will hopefully allow us to detect if more
    // or less cross shard calls are being made for a fetch.
    co_return co_await fetch_from(multi_partition_req_config(t));
}
