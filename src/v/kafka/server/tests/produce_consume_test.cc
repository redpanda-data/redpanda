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
#include "kafka/protocol/offset_for_leader_epoch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/snc_quota_manager.h"
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

namespace ch = std::chrono;

struct throughput_limits_fixure : prod_consume_fixture {
    ch::milliseconds _window_width;
    ch::milliseconds _balancer_period;
    int64_t _rate_minimum;

    void config_set(const std::string_view name, const std::any value) {
        ss::smp::invoke_on_all([&] {
            config::shard_local_cfg().get(name).set_value(value);
        }).get0();
    }

    void config_set_window_width(const ch::milliseconds window_width) {
        _window_width = window_width;
        ss::smp::invoke_on_all([window_width] {
            config::shard_local_cfg()
              .get("kafka_quota_balancer_window_ms")
              .set_value(window_width);
        }).get0();
    }

    void config_set_balancer_period(const ch::milliseconds balancer_period) {
        _balancer_period = balancer_period;
        ss::smp::invoke_on_all([balancer_period] {
            config::shard_local_cfg()
              .get("kafka_quota_balancer_node_period_ms")
              .set_value(balancer_period);
        }).get0();
    }

    void config_set_rate_minimum(const int64_t rate_minimum) {
        _rate_minimum = rate_minimum;
        ss::smp::invoke_on_all([rate_minimum] {
            config::shard_local_cfg()
              .get("kafka_quota_balancer_min_shard_throughput_bps")
              .set_value(rate_minimum);
        }).get0();
    }

    int warmup_cycles(const size_t rate_limit, const size_t packet_size) const {
        // warmup is the number of iterations enough to exhaust the token bucket
        // at least twice, and for the balancer to run at least 4 times after
        // that.
        const int warmup_bytes = rate_limit
                                 * ch::duration_cast<ch::milliseconds>(
                                     // token bucket component
                                     2 * _window_width
                                     // balancer component
                                     + 4 * _balancer_period)
                                     .count()
                                 / 1000;
        return warmup_bytes / packet_size + 1;
    }

    size_t test_ingress(
      const size_t rate_limit_in,
      const size_t batch_size,
      const int tolerance_percent) {
        size_t kafka_in_data_len = 0;
        constexpr size_t kafka_packet_overhead = 127;
        // do not divide rate by smp::count because
        // - balanced case: TP will be balanced and  the entire quota will end
        // up in one shard
        // - static case: rate_limit is per shard
        const auto batches_cnt = /* 1s * */ rate_limit_in
                                 / (batch_size + kafka_packet_overhead);
        ch::steady_clock::time_point start;
        ch::milliseconds throttle_time{};

        for (int k = -warmup_cycles(
               rate_limit_in, batch_size + kafka_packet_overhead);
             k != batches_cnt;
             ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                throttle_time = {};
                BOOST_TEST_WARN(
                  false,
                  "Ingress measurement starts. batches: " << batches_cnt);
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
        const auto rate_estimated = rate_limit_in
                                    - _rate_minimum * (ss::smp::count - 1);
        const auto time_estimated = ch::milliseconds(
          wire_data_length * 1000 / rate_estimated);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated)
            < time_estimated * tolerance_percent / 100,
          "Ingress time: stop-start["
            << stop - start << "] ≈ time_estimated[" << time_estimated << "] ±"
            << tolerance_percent << "%, error: " << std::setprecision(3)
            << (stop - start - time_estimated) * 100.0 / time_estimated << "%");
        return kafka_in_data_len;
    }

    size_t test_egress(
      const size_t kafka_data_available,
      const size_t rate_limit_out,
      const size_t batch_size,
      const int tolerance_percent) {
        size_t kafka_out_data_len = 0;
        constexpr size_t kafka_packet_overhead = 62;
        ch::steady_clock::time_point start;
        size_t total_size{};
        ch::milliseconds throttle_time{};
        // consume cannot be measured by the number of fetches because the size
        // of fetch payload is up to redpanda, "fetch_max_bytes" is merely a
        // guidance. Therefore the consume test runs as long as there is data
        // to fetch. We only can consume almost as much as have been produced:
        const auto kafka_data_cap = kafka_data_available - batch_size * 2;
        for (int k = -warmup_cycles(
               rate_limit_out, batch_size + kafka_packet_overhead);
             kafka_out_data_len < kafka_data_cap;
             ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                total_size = {};
                throttle_time = {};
                BOOST_TEST_WARN(
                  false,
                  "Egress measurement starts. kafka_out_data_len: "
                    << kafka_out_data_len
                    << ", kafka_data_cap: " << kafka_data_cap);
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
        const auto rate_estimated = rate_limit_out
                                    - _rate_minimum * (ss::smp::count - 1);
        const auto time_estimated = ch::milliseconds(
          total_size * 1000 / rate_estimated);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated)
            < (time_estimated * tolerance_percent / 100),
          "Egress time: stop-start["
            << stop - start << "] ≈ time_estimated[" << time_estimated << "] ±"
            << tolerance_percent << "%, error: " << std::setprecision(3)
            << (stop - start - time_estimated) * 100.0 / time_estimated << "%");
        return kafka_out_data_len;
    }
};

FIXTURE_TEST(test_node_throughput_limits_static, throughput_limits_fixure) {
    // configure
    constexpr int64_t pershard_rate_limit_in = 9_KiB;
    constexpr int64_t pershard_rate_limit_out = 7_KiB;
    constexpr size_t batch_size = 256;
    config_set(
      "kafka_throughput_limit_node_in_bps",
      std::make_optional(pershard_rate_limit_in * ss::smp::count));
    config_set(
      "kafka_throughput_limit_node_out_bps",
      std::make_optional(pershard_rate_limit_out * ss::smp::count));
    config_set("fetch_max_bytes", batch_size);
    config_set("max_kafka_throttle_delay_ms", 30'000ms);
    config_set_window_width(200ms);
    config_set_balancer_period(0ms);
    config_set_rate_minimum(0);

    wait_for_controller_leadership().get();
    start();

    // PRODUCE 10 KiB in smaller batches, check throttle but do not honour it,
    // check that has to take 1 s
    const size_t kafka_in_data_len = test_ingress(
      pershard_rate_limit_in, batch_size, 3 /*%*/);

    // CONSUME
    const size_t kafka_out_data_len = test_egress(
      kafka_in_data_len, pershard_rate_limit_out, batch_size, 3 /*%*/);

    // otherwise test is not valid:
    BOOST_REQUIRE_GT(kafka_in_data_len, kafka_out_data_len);
}

FIXTURE_TEST(test_node_throughput_limits_balanced, throughput_limits_fixure) {
    // configure
    constexpr int64_t rate_limit_in = 9_KiB;
    constexpr int64_t rate_limit_out = 7_KiB;
    constexpr size_t batch_size = 256;
    config_set(
      "kafka_throughput_limit_node_in_bps", std::make_optional(rate_limit_in));
    config_set(
      "kafka_throughput_limit_node_out_bps",
      std::make_optional(rate_limit_out));
    config_set("fetch_max_bytes", batch_size);
    config_set("max_kafka_throttle_delay_ms", 30'000ms);
    config_set("kafka_quota_balancer_min_shard_throughput_ratio", 0.);
    config_set_window_width(100ms);
    config_set_balancer_period(50ms);
    config_set_rate_minimum(250);

    wait_for_controller_leadership().get();
    start();

    // PRODUCE 10 KiB in smaller batches, check throttle but do not honour it,
    // check that has to take 1 s
    const size_t kafka_in_data_len = test_ingress(
      rate_limit_in, batch_size, 8 /*%*/);

    // CONSUME
    size_t kafka_out_data_len = test_egress(
      kafka_in_data_len, rate_limit_out, batch_size, 8 /*%*/);

    // otherwise test is not valid:
    BOOST_REQUIRE_GT(kafka_in_data_len, kafka_out_data_len);

    if (ss::smp::count <= 2) {
        // the following tests are only valid when shards count is greater
        // than the # of partintions produced to / consumed from
        return;
    }

    const auto collect_quotas_minmax =
      [this](
        std::function<kafka::snc_quota_manager::quota_t(
          const kafka::snc_quota_manager&)> mapper,
        const char* const direction) {
          const auto quotas = app.snc_quota_mgr.map(std::move(mapper)).get0();
          info("quotas_{}: {}", direction, quotas);
          const auto quotas_minmax = std::minmax_element(
            quotas.cbegin(), quotas.cend());
          return std::pair{*quotas_minmax.first, *quotas_minmax.second};
      };

    const auto quotas_b_minmax_in = collect_quotas_minmax(
      [](const kafka::snc_quota_manager& qm) { return qm.get_quota().in; },
      "in");
    const auto quotas_b_minmax_eg = collect_quotas_minmax(
      [](const kafka::snc_quota_manager& qm) { return qm.get_quota().eg; },
      "eg");

    // verify that the effective quota is distributed very unevenly
    BOOST_CHECK_GT(quotas_b_minmax_in.second, quotas_b_minmax_in.first * 5);
    BOOST_CHECK_GT(quotas_b_minmax_eg.second, quotas_b_minmax_eg.first * 5);

    // verify that the minimum quota has been honoured
    BOOST_CHECK_GE(quotas_b_minmax_in.first, _rate_minimum);
    BOOST_CHECK_GE(quotas_b_minmax_in.first, _rate_minimum);

    // disable the balancer; that should reset effective quotas to default
    config_set_balancer_period(0ms);

    const auto quotas_e_minmax_in = collect_quotas_minmax(
      [](const kafka::snc_quota_manager& qm) { return qm.get_quota().in; },
      "in");
    const auto quotas_e_minmax_eg = collect_quotas_minmax(
      [](const kafka::snc_quota_manager& qm) { return qm.get_quota().eg; },
      "eg");

    // when the balancer is off, effective quotas is reset to default quotas
    // which are evenly distributed across shards: max-min<=1
    BOOST_CHECK_LE(quotas_e_minmax_in.second, quotas_e_minmax_in.first + 1);
    BOOST_CHECK_LE(quotas_e_minmax_eg.second, quotas_e_minmax_eg.first + 1);
}

FIXTURE_TEST(test_quota_balancer_config_balancer_period, prod_consume_fixture) {
    namespace ch = std::chrono;

    auto get_balancer_runs = [this] {
        return app.snc_quota_mgr
          .map_reduce0(
            [](const kafka::snc_quota_manager& qm) {
                return qm.get_snc_quotas_probe().get_balancer_runs();
            },
            0,
            [](uint32_t lhs, uint32_t rhs) { return lhs + rhs; })
          .get0();
    };

    auto set_balancer_period = [](const ch::milliseconds d) {
        ss::smp::invoke_on_all([&] {
            auto& config = config::shard_local_cfg();
            config.get("kafka_quota_balancer_node_period_ms").set_value(d);
        }).get0();
    };

    wait_for_controller_leadership().get();

    set_balancer_period(25ms);
    BOOST_TEST_WARN(false, "Starting");
    ss::sleep(100ms).get0();
    int br = get_balancer_runs();
    BOOST_TEST_CHECK(
      abs(br - 4) <= 1, "Expected 4±1 balancer runs, got " << br);

    set_balancer_period(0ms);
    int br_last = get_balancer_runs();
    ss::sleep(100ms).get0();
    br = get_balancer_runs();
    BOOST_TEST_CHECK(
      abs(br - br_last - 0) <= 1,
      "Expected 0±1 balancer runs, got " << br - br_last);

    set_balancer_period(15ms);
    br_last = get_balancer_runs();
    ss::sleep(100ms).get0();
    br = get_balancer_runs();
    BOOST_TEST_CHECK(
      abs(br - br_last - 7) <= 1,
      "Expected 7±1 balancer runs, got " << br - br_last);
}

// TODO: move producer utilities somewhere else and give this test a proper
// home.
FIXTURE_TEST(test_offset_for_leader_epoch, prod_consume_fixture) {
    producer = std::make_unique<kafka::client::transport>(
      make_kafka_client().get0());
    producer->connect().get0();
    model::topic_namespace tp_ns(model::ns("kafka"), test_topic);
    add_topic(tp_ns).get0();
    model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
    tests::cooperative_spin_wait_with_timeout(10s, [ntp, this] {
        auto shard = app.shard_table.local().shard_for(ntp);
        if (!shard) {
            return ss::make_ready_future<bool>(false);
        }
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& pm) {
              return pm.get(ntp)->is_leader();
          });
    }).get0();
    auto shard = app.shard_table.local().shard_for(ntp);
    for (int i = 0; i < 3; i++) {
        // Refresh leadership.
        app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                auto raft = mgr.get(ntp)->raft();
                raft->step_down("force_step_down").get();
                tests::cooperative_spin_wait_with_timeout(10s, [raft] {
                    return raft->is_leader();
                }).get0();
            })
          .get();
        app.partition_manager
          .invoke_on(
            *shard,
            [this, ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                produce([this](size_t cnt) {
                    return small_batches(cnt);
                }).get0();
            })
          .get();
    }
    // Prefix truncate the log so the beginning of the log moves forward.
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            auto partition = mgr.get(ntp);
            storage::truncate_prefix_config cfg(
              model::offset(1), ss::default_priority_class());
            partition->log().truncate_prefix(cfg).get();
        })
      .get();

    // Make a request getting the offset from a term below the start of the
    // log.
    auto client = make_kafka_client().get0();
    client.connect().get();
    auto current_term = app.partition_manager
                          .invoke_on(
                            *shard,
                            [ntp](cluster::partition_manager& mgr) {
                                return mgr.get(ntp)->raft()->term();
                            })
                          .get();
    kafka::offset_for_leader_epoch_request req;
    kafka::offset_for_leader_topic t{
      test_topic,
      {{model::partition_id(0),
        kafka::leader_epoch(current_term()),
        kafka::leader_epoch(0)}},
      {},
    };
    req.data.topics.emplace_back(std::move(t));
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();
    BOOST_REQUIRE_EQUAL(1, resp.data.topics.size());
    const auto& topic_resp = resp.data.topics[0];
    BOOST_REQUIRE_EQUAL(1, topic_resp.partitions.size());
    const auto& partition_resp = topic_resp.partitions[0];

    BOOST_REQUIRE_NE(partition_resp.end_offset, model::offset(-1));

    // Check that the returned offset is the start of the log, since the
    // requested term has been truncated.
    auto earliest_kafka_offset
      = app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                auto start_offset = partition->log().offsets().start_offset;
                return partition->get_offset_translator_state()
                  ->from_log_offset(start_offset);
            })
          .get();
    BOOST_REQUIRE_EQUAL(earliest_kafka_offset, partition_resp.end_offset);
}
