// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/transport.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/server/handlers/produce.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;

struct prod_consume_fixture : public redpanda_thread_fixture {
    void start() {
        consumer = std::make_unique<kafka::client::transport>(
          make_kafka_client().get0());
        producer = std::make_unique<kafka::client::transport>(
          make_kafka_client().get0());
        consumer->connect().get0();
        producer->connect().get0();
        model::topic_namespace tp_ns(model::ns("kafka"), test_topic);
        add_topic(tp_ns).get0();
        model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
        tests::cooperative_spin_wait_with_timeout(2s, [ntp, this] {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp)->is_leader();
              });
        }).get0();
    }

    std::vector<kafka::produce_request::partition> small_batches(size_t count) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        for (int i = 0; i < count; ++i) {
            iobuf v{};
            v.append("v", 1);
            builder.add_raw_kv(iobuf{}, std::move(v));
        }

        std::vector<kafka::produce_request::partition> res;

        kafka::produce_request::partition partition;
        partition.partition_index = model::partition_id(0);
        partition.records.emplace(std::move(builder).build());
        res.push_back(std::move(partition));
        return res;
    }

    ss::future<kafka::produce_response>
    produce_raw(std::vector<kafka::produce_request::partition>&& partitions) {
        kafka::produce_request::topic tp;
        tp.partitions = std::move(partitions);
        tp.name = test_topic;
        std::vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer->dispatch(std::move(req));
    }

    template<typename T>
    ss::future<model::offset> produce(T&& batch_factory) {
        const size_t count = random_generators::get_int(1, 20);
        return produce_raw(batch_factory(count))
          .then([count](kafka::produce_response r) {
              return r.data.responses.begin()->partitions.begin()->base_offset
                     + model::offset(count - 1);
          });
    }

    ss::future<kafka::fetch_response> fetch_next() {
        kafka::fetch_request::partition partition;
        partition.fetch_offset = fetch_offset;
        partition.partition_index = model::partition_id(0);
        partition.log_start_offset = model::offset(0);
        partition.max_bytes = 1_MiB;
        kafka::fetch_request::topic topic;
        topic.name = test_topic;
        topic.fetch_partitions.push_back(partition);

        kafka::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 10_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));

        return consumer->dispatch(std::move(req), kafka::api_version(4))
          .then([this](kafka::fetch_response resp) {
              if (resp.data.topics.empty()) {
                  return resp;
              }
              auto& part = *resp.data.topics.begin();

              for ([[maybe_unused]] auto& r : part.partitions) {
                  const auto& data = part.partitions.begin()->records;
                  if (data && !data->empty()) {
                      // update next fetch offset the same way as Kafka clients
                      fetch_offset = ++data->last_offset();
                  }
              }
              return resp;
          });
    }

    model::offset fetch_offset{0};
    std::unique_ptr<kafka::client::transport> consumer;
    std::unique_ptr<kafka::client::transport> producer;
    ss::abort_source as;
    const model::topic test_topic = model::topic("test-topic");
};

/**
 * produce/consume test simulating Hazelcast benchmart workload with small
 * batches.
 */
FIXTURE_TEST(test_produce_consume_small_batches, prod_consume_fixture) {
    wait_for_controller_leadership().get0();
    start();
    auto offset_1 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get0();
    auto resp_1 = fetch_next().get0();

    auto offset_2 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get0();
    auto resp_2 = fetch_next().get0();

    BOOST_REQUIRE_EQUAL(resp_1.data.topics.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_2.data.topics.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_1.data.topics.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.topics.begin()->partitions.begin()->error_code,
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.topics.begin()->partitions.begin()->records->last_offset(),
      offset_1);
    BOOST_REQUIRE_EQUAL(resp_2.data.topics.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.topics.begin()->partitions.begin()->error_code,
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.topics.begin()->partitions.begin()->records->last_offset(),
      offset_2);
};

FIXTURE_TEST(test_version_handler, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    std::vector<kafka::produce_request::topic> topics;
    topics.push_back(kafka::produce_request::topic{
      .name = model::topic{"abc123"}, .partitions = small_batches(10)});

    const auto unsupported_version = kafka::api_version(
      kafka::produce_handler::max_supported() + 1);
    BOOST_CHECK_THROW(
      producer
        ->dispatch(
          // NOLINTNEXTLINE(bugprone-use-after-move)
          kafka::produce_request(std::nullopt, 1, std::move(topics)),
          unsupported_version)
        .get(),
      kafka::client::kafka_request_disconnected_exception);
}

static std::vector<kafka::produce_request::partition>
single_batch(const size_t volume) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    {
        const ss::sstring data(volume, 's');
        iobuf v{};
        v.append(data.data(), data.size());
        builder.add_raw_kv(iobuf{}, std::move(v));
    }

    kafka::produce_request::partition partition;
    partition.partition_index = model::partition_id(0);
    partition.records.emplace(std::move(builder).build());

    std::vector<kafka::produce_request::partition> res;
    res.push_back(std::move(partition));
    return res;
}

FIXTURE_TEST(test_node_throughput_limits, prod_consume_fixture) {
    namespace ch = std::chrono;

    // configure
    constexpr uint64_t pershard_rate_limit_in = 9_KiB;
    constexpr uint64_t pershard_rate_limit_out = 7_KiB;
    constexpr auto window_width = 200ms;
    constexpr size_t batch_size = 256;
    ss::smp::invoke_on_all([&] {
        auto& config = config::shard_local_cfg();
        config.get("kafka_throughput_limit_node_in_bps")
          .set_value(
            std::make_optional(pershard_rate_limit_in * ss::smp::count));
        config.get("kafka_throughput_limit_node_out_bps")
          .set_value(
            std::make_optional(pershard_rate_limit_out * ss::smp::count));
        config.get("kafka_quota_balancer_window_ms").set_value(window_width);
        config.get("fetch_max_bytes").set_value(batch_size);
        config.get("max_kafka_throttle_delay_ms").set_value(60'000ms);
    }).get0();
    wait_for_controller_leadership().get();
    start();

    // PRODUCE 10 KiB in smaller batches, check throttle but do not honour it,
    // check that has to take 1 s
    size_t kafka_in_data_len = 0;
    {
        constexpr size_t kafka_packet_overhead = 127;
        const auto batches_cnt = pershard_rate_limit_in
                                 / (batch_size + kafka_packet_overhead);
        ch::steady_clock::time_point start;
        ch::milliseconds throttle_time{};
        // warmup is the number of iterations enough to exhaust the token bucket
        // at least twice
        const int warmup
          = 2 * pershard_rate_limit_in
              * ch::duration_cast<ch::milliseconds>(window_width).count() / 1000
              / (batch_size + kafka_packet_overhead)
            + 1;
        for (int k = -warmup; k != batches_cnt; ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                throttle_time = {};
            }
            throttle_time += produce_raw(single_batch(batch_size))
                               .then([](const kafka::produce_response& r) {
                                   return r.data.throttle_time_ms;
                               })
                               .get0();
            kafka_in_data_len += batch_size;
        }
        const auto stop = ch::steady_clock::now();
        const auto wire_data_length = (batch_size + kafka_packet_overhead)
                                      * batches_cnt;
        const auto time_estimated = ch::milliseconds(
          wire_data_length * 1000 / pershard_rate_limit_in);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated) < time_estimated / 25,
          "stop-start[" << stop - start << "] == time_estimated["
                        << time_estimated << "] ±4%");
    }

    // CONSUME
    size_t kafka_out_data_len = 0;
    {
        constexpr size_t kafka_packet_overhead = 62;
        ch::steady_clock::time_point start;
        size_t total_size{};
        ch::milliseconds throttle_time{};
        const int warmup
          = 2 * pershard_rate_limit_out
              * ch::duration_cast<ch::milliseconds>(window_width).count() / 1000
              / (batch_size + kafka_packet_overhead)
            + 1;
        // consume cannot be measured by the number of fetches because the size
        // of fetch payload is up to redpanda, "fetch_max_bytes" is merely a
        // guidance. Therefore the consume test runs as long as there is data
        // to fetch. We only can consume almost as much as have been produced:
        const auto kafka_data_cap = kafka_in_data_len - batch_size * 2;
        for (int k = -warmup; kafka_out_data_len < kafka_data_cap; ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                total_size = {};
                throttle_time = {};
            }
            const auto fetch_resp = fetch_next().get0();
            BOOST_REQUIRE_EQUAL(fetch_resp.data.topics.size(), 1);
            BOOST_REQUIRE_EQUAL(fetch_resp.data.topics[0].partitions.size(), 1);
            BOOST_TEST_REQUIRE(
              fetch_resp.data.topics[0].partitions[0].records.has_value());
            const auto kafka_data_len = fetch_resp.data.topics[0]
                                          .partitions[0]
                                          .records.value()
                                          .size_bytes();
            total_size += kafka_data_len + kafka_packet_overhead;
            throttle_time += fetch_resp.data.throttle_time_ms;
            kafka_out_data_len += kafka_data_len;
        }
        const auto stop = ch::steady_clock::now();
        const auto time_estimated = ch::milliseconds(
          total_size * 1000 / pershard_rate_limit_out);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated) < time_estimated / 25,
          "stop-start[" << stop - start << "] == time_estimated["
                        << time_estimated << "] ±4%");
    }

    // otherwise test is not valid:
    BOOST_REQUIRE_GT(kafka_in_data_len, kafka_out_data_len);
}
