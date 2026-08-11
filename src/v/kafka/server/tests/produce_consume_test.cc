// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/offset_for_leader_epoch.h"
#include "kafka/protocol/produce.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/snc_quota_manager.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/metrics_types.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/bool_class.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <vector>

using namespace std::chrono_literals;
using std::vector;
using tests::kv_t;

struct prod_consume_fixture : public redpanda_thread_fixture {
    void start(unsigned int count = 1) {
        producers.reserve(count);
        consumers.reserve(count);
        fetch_offsets.resize(count, model::offset{0});

        add_topic(test_tp_ns, static_cast<int>(count)).get();

        ss::parallel_for_each(boost::irange(0u, count), [&](auto i) {
            consumers.emplace_back(make_kafka_client().get());
            auto& consumer = consumers.back();

            producers.emplace_back(make_kafka_client().get());
            auto& producer = producers.back();

            model::ntp ntp(
              test_tp_ns.ns, test_tp_ns.tp, model::partition_id(i));
            return ss::when_all_succeed(
                     producer.connect(),
                     consumer.connect(),
                     tests::cooperative_spin_wait_with_timeout(
                       2s,
                       [ntp, this] {
                           auto shard = app.shard_table.local().shard_for(ntp);
                           if (!shard) {
                               return ss::make_ready_future<bool>(false);
                           }
                           return app.partition_manager.invoke_on(
                             *shard, [ntp](cluster::partition_manager& pm) {
                                 return pm.get(ntp)->is_leader();
                             });
                       }))
              .discard_result();
        }).get();
    }

    chunked_vector<kafka::produce_request::partition>
    small_batches(size_t count) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        for (size_t i = 0; i < count; ++i) {
            iobuf v{};
            v.append("v", 1);
            builder.add_raw_kv(iobuf{}, std::move(v));
        }

        chunked_vector<kafka::produce_request::partition> res;

        kafka::produce_request::partition partition;
        partition.partition_index = model::partition_id(0);
        partition.records.emplace(std::move(builder).build());
        res.push_back(std::move(partition));
        return res;
    }

    ss::future<kafka::produce_response> produce_raw(
      kafka::client::transport& producer,
      chunked_vector<kafka::produce_request::partition>&& partitions) {
        kafka::produce_request::topic tp;
        tp.partitions = std::move(partitions);
        tp.name = test_topic;
        chunked_vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer.dispatch(std::move(req), kafka::api_version(7));
    }

    ss::future<kafka::produce_response> produce_raw(
      chunked_vector<kafka::produce_request::partition>&& partitions) {
        return produce_raw(producers.front(), std::move(partitions));
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

    ss::future<kafka::fetch_response>
    fetch_next(kafka::client::transport& consumer, model::partition_id p_id) {
        kafka::fetch_request::partition partition;
        partition.fetch_offset = fetch_offsets[p_id()];
        partition.partition = p_id;
        partition.log_start_offset = model::offset(0);
        partition.partition_max_bytes = 1_MiB;
        kafka::fetch_request::topic topic;
        topic.topic = test_topic;
        topic.partitions.push_back(partition);

        kafka::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 10_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));

        return consumer.dispatch(std::move(req), kafka::api_version(4))
          .then([this, p_id](kafka::fetch_response resp) {
              if (resp.data.responses.empty()) {
                  return resp;
              }
              auto& part = *resp.data.responses.begin();

              for ([[maybe_unused]] auto& r : part.partitions) {
                  const auto& data = part.partitions.begin()->records;
                  if (data && !data->empty()) {
                      // update next fetch offset the same way as Kafka clients
                      fetch_offsets[p_id()] = ++data->last_offset();
                  }
              }
              return resp;
          });
    }

    ss::future<kafka::fetch_response> fetch_next() {
        return fetch_next(consumers.front(), model::partition_id{0});
    }

    kafka::kafka_probe& kafka_probe() {
        return app._kafka_server.local().kafka_probe();
    }

    uint32_t produce_bad_ts_count() {
        return kafka_probe()._produce_bad_create_time;
    };

    uint64_t bytes_by_compression(model::compression compression_type) {
        return kafka_probe()._bytes_by_compression.at((size_t)compression_type);
    }

    ~prod_consume_fixture() {
        ss::parallel_for_each(consumers, [](kafka::client::transport& t) {
            return t.stop();
        }).get();
        ss::parallel_for_each(producers, [](kafka::client::transport& t) {
            return t.stop();
        }).get();
    }

    std::vector<model::offset> fetch_offsets;
    std::vector<kafka::client::transport> consumers;
    std::vector<kafka::client::transport> producers;
    ss::abort_source as;
    const model::topic_namespace test_tp_ns = {
      model::ns("kafka"), model::topic("test-topic")};
    const model::topic& test_topic = test_tp_ns.tp;
};

/**
 * produce/consume test simulating Hazelcast benchmart workload with small
 * batches.
 */
FIXTURE_TEST(test_produce_consume_small_batches, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    auto offset_1 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get();
    auto resp_1 = fetch_next().get();

    auto offset_2 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get();
    auto resp_2 = fetch_next().get();

    BOOST_REQUIRE_EQUAL(resp_1.data.responses.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_2.data.responses.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.responses.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.responses.begin()->partitions.begin()->error_code,
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.responses.begin()->partitions.begin()->records->last_offset(),
      offset_1);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.responses.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.responses.begin()->partitions.begin()->error_code,
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.responses.begin()->partitions.begin()->records->last_offset(),
      offset_2);
};

FIXTURE_TEST(test_version_handler, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    chunked_vector<kafka::produce_request::topic> topics;
    topics.push_back(
      kafka::produce_request::topic{
        .name = model::topic{"abc123"}, .partitions = small_batches(10)});

    const auto unsupported_version = kafka::api_version(
      kafka::produce_handler::max_supported() + 1);
    BOOST_CHECK_THROW(
      producers.front()
        .dispatch(
          // NOLINTNEXTLINE(bugprone-use-after-move)
          kafka::produce_request(std::nullopt, 1, std::move(topics)),
          unsupported_version)
        .get(),
      kafka::client::kafka_request_disconnected_exception);
}

static chunked_vector<kafka::produce_request::partition>
single_batch(model::partition_id p_id, const size_t volume) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    {
        const ss::sstring data(volume, 's');
        iobuf v{};
        v.append(data.data(), data.size());
        builder.add_raw_kv(iobuf{}, std::move(v));
    }

    kafka::produce_request::partition partition;
    partition.partition_index = p_id;
    partition.records.emplace(std::move(builder).build());

    chunked_vector<kafka::produce_request::partition> res;
    res.push_back(std::move(partition));
    return res;
}

namespace ch = std::chrono;

namespace {

enum class execution { seq, par };

/// Runs func in a thread on the given shard
template<typename Func>
auto async_submit_to(unsigned int shard, Func&& func) {
    return ss::smp::submit_to(shard, [&func, shard]() {
        return ss::async(std::forward<Func>(func), shard);
    });
}

/// std::transform_reduce, but with an api that matches ss::map_reduce
///
/// Mapper must be synchronous
template<typename Range, typename Mapper, typename Initial, typename Reduce>
auto transform_reduce(
  Range&& rng, Mapper&& mapper, Initial&& initial, Reduce&& reduce) {
    return std::transform_reduce(
      rng.begin(),
      rng.end(),
      std::forward<Initial>(initial),
      std::forward<Reduce>(reduce),
      std::forward<Mapper>(mapper));
}

template<typename Mapper, typename Initial, typename Reduce>
auto map_reduce_thread_per_core(
  Mapper&& mapper, Initial&& initial, Reduce&& reduce) {
    return ss::map_reduce(
      boost::irange(0u, ss::smp::count),
      [&mapper](auto shard) {
          return async_submit_to(shard, std::forward<Mapper>(mapper));
      },
      std::forward<Initial>(initial),
      std::forward<Reduce>(reduce));
}

template<typename Mapper, typename Initial, typename Reduce>
auto transform_reduce_thread_per_core(
  Mapper&& mapper, Initial&& initial, Reduce&& reduce) {
    return transform_reduce(
      boost::irange(0u, ss::smp::count),
      [&mapper](auto shard) {
          return async_submit_to(shard, std::forward<Mapper>(mapper)).get();
      },
      std::forward<Initial>(initial),
      std::forward<Reduce>(reduce));
}

/// Run mapper in a thread on each core, and then reduce on the original core,
/// returning a synchrous result
///
/// execution::par - Run mapper on each core in parallel
/// execution::par - Run mapper on each core sequentially
template<typename Mapper, typename Initial, typename Reduce>
auto transform_reduce_thread_per_core(
  execution policy, Mapper&& mapper, Initial&& initial, Reduce&& reduce) {
    switch (policy) {
    case execution::seq:
        return transform_reduce_thread_per_core(
          std::forward<Mapper>(mapper),
          std::forward<Initial>(initial),
          std::forward<Reduce>(reduce));
    case execution::par:
        return map_reduce_thread_per_core(
                 std::forward<Mapper>(mapper),
                 std::forward<Initial>(initial),
                 std::forward<Reduce>(reduce))
          .get();
    }
}

/// Return a tuple of the result of applying BinaryOp element-wise to each tuple
template<typename BinaryOp>
struct tuple_binary_op {
    auto operator()(auto&& t1, auto&& t2) const {
        return std::apply(
          [&](auto&&... args1) {
              return std::apply(
                [&](auto&&... args2) {
                    return std::make_tuple(
                      BinaryOp{}(
                        std::forward<decltype(args1)>(args1),
                        std::forward<decltype(args2)>(args2))...);
                },
                std::forward<decltype(t2)>(t2));
          },
          std::forward<decltype(t1)>(t1));
    }
};

} // namespace

struct throughput_limits_fixure : prod_consume_fixture {
    using honour_throttle = ss::bool_class<class hounour_throttle_tag>;

    static constexpr size_t kafka_packet_in_overhead = 127;
    static constexpr size_t kafka_packet_eg_overhead = 62;

    void config_set(const std::string_view name, const std::any value) {
        ss::smp::invoke_on_all([&] {
            config::shard_local_cfg().get(name).set_value(value);
        }).get();
    }

    auto do_produce(
      int i,
      size_t batches_cnt,
      size_t batch_size,
      honour_throttle honour_throttle) {
        const model::partition_id p_id{i};
        size_t data_len{0};
        size_t total_len{0};
        ch::milliseconds total_throttle_time{};
        ch::milliseconds throttle_time{};
        for (size_t k{0}; k != batches_cnt; ++k) {
            if (honour_throttle && throttle_time > ch::milliseconds::zero()) {
                ss::sleep(throttle_time).get();
            }
            auto res
              = produce_raw(producers[i], single_batch(p_id, batch_size)).get();
            throttle_time = res.data.throttle_time_ms;
            total_throttle_time += throttle_time;
            data_len += batch_size;
            total_len += batch_size + kafka_packet_in_overhead;
        }
        return std::make_tuple(data_len, total_len, total_throttle_time);
    }

    auto do_consume(int i, size_t data_cap, honour_throttle honour_throttle) {
        const model::partition_id p_id{i};
        size_t data_len{0};
        size_t total_len{0};
        ch::milliseconds total_throttle_time{};
        ch::milliseconds throttle_time{};
        while (data_len < data_cap) {
            if (honour_throttle && throttle_time > ch::milliseconds::zero()) {
                ss::sleep(throttle_time).get();
            }
            const auto fetch_resp = fetch_next(consumers[i], p_id).get();
            BOOST_REQUIRE_EQUAL(fetch_resp.data.responses.size(), 1);
            BOOST_REQUIRE_EQUAL(
              fetch_resp.data.responses[0].partitions.size(), 1);
            BOOST_TEST_REQUIRE(
              fetch_resp.data.responses[0].partitions[0].records.has_value());
            const auto kafka_data_len = fetch_resp.data.responses[0]
                                          .partitions[0]
                                          .records.value()
                                          .size_bytes();
            throttle_time = fetch_resp.data.throttle_time_ms;
            total_throttle_time += throttle_time;
            data_len += kafka_data_len;
            total_len += kafka_data_len + kafka_packet_eg_overhead;
        }
        return std::make_tuple(data_len, total_len, total_throttle_time);
    }

    auto get_recorded_traffic() {
        return app.snc_quota_mgr
          .map_reduce0(
            [](kafka::snc_quota_manager& snc) {
                return std::make_tuple(
                  snc.get_snc_quotas_probe().get_traffic_in(),
                  snc.get_snc_quotas_probe().get_traffic_eg());
            },
            std::make_tuple(size_t{0}, size_t{0}),
            tuple_binary_op<std::plus<>>{})
          .get();
    };

    auto get_throttle_time() {
        return app.snc_quota_mgr
          .map_reduce0(
            [](kafka::snc_quota_manager& snc) {
                return snc.get_snc_quotas_probe().get_throttle_time();
            },
            ss::metrics::histogram{},
            std::plus{})
          .get();
    };

    void test_throughput(honour_throttle honour_throttle, execution policy) {
        using clock = kafka::snc_quota_manager::clock;
        // configure
        constexpr int64_t rate_limit_in = 9_KiB;
        constexpr int64_t rate_limit_out = 7_KiB;
        constexpr size_t batch_size = 256;
        constexpr auto max_rate = std::max(rate_limit_in, rate_limit_out);
        constexpr auto min_rate = std::min(rate_limit_in, rate_limit_out);
        const double expected_max_throttle
          = (double(batch_size) / min_rate)
            * (policy == execution::seq ? 1 : ss::smp::count)
            * (honour_throttle ? 1 : 2);
        const size_t tolerance_percent = 8;
        config_set(
          "kafka_throughput_limit_node_in_bps",
          std::make_optional(rate_limit_in));
        config_set(
          "kafka_throughput_limit_node_out_bps",
          std::make_optional(rate_limit_out));
        config_set("fetch_max_bytes", batch_size);
        config_set("max_kafka_throttle_delay_ms", 30'000ms);

        wait_for_controller_leadership().get();
        start(ss::smp::count);

        // PRODUCE smaller batches for 5s per client
        const auto batches_cnt = (5s).count() * rate_limit_in
                                 / (batch_size + kafka_packet_in_overhead);

        constexpr auto now = []() {
            clock::update();
            return clock::now();
        };

        BOOST_TEST_MESSAGE("Warming up");
        ss::sleep(
          produce_raw(
            producers[0], single_batch(model::partition_id{0}, max_rate))
            .get()
            .data.throttle_time_ms)
          .get();

        auto [init_recorded_in, init_recorded_eg] = get_recorded_traffic();

        BOOST_TEST_MESSAGE("Producing");
        auto start_in = now();
        auto [kafka_in_data_len, kafka_in_total_len, throttle_time_in]
          = transform_reduce_thread_per_core(
            policy,
            [&](auto i) {
                return do_produce(i, batches_cnt, batch_size, honour_throttle);
            },
            std::make_tuple(size_t{0}, size_t{0}, ch::milliseconds{0}),
            tuple_binary_op<std::plus<>>{});
        auto duration_in = now() - start_in;
        auto [produce_recorded_in, produce_recorded_eg]
          = tuple_binary_op<std::minus<>>{}(
            get_recorded_traffic(),
            std::make_tuple(init_recorded_in, init_recorded_eg));

        BOOST_TEST_MESSAGE("Finished Producing, Warming up");

        // Consume the warmup batch and wait for throttle time
        ss::sleep(fetch_next(consumers[0], model::partition_id{0})
                    .get()
                    .data.throttle_time_ms)
          .get();
        std::tie(init_recorded_in, init_recorded_eg) = get_recorded_traffic();

        BOOST_TEST_MESSAGE("Consuming");
        // CONSUME
        auto start_eg = now();
        auto [kafka_eg_data_len, kafka_eg_total_len, throttle_time_out]
          = transform_reduce_thread_per_core(
            policy,
            [&](auto i) {
                const model::partition_id p_id(i);
                const auto data_cap = (kafka_in_data_len / ss::smp::count)
                                      - batch_size * 2;
                return do_consume(i, data_cap, honour_throttle);
            },
            std::make_tuple(size_t{0}, size_t{0}, ch::milliseconds{0}),
            tuple_binary_op<std::plus<>>{});
        auto duration_eg = now() - start_eg;
        BOOST_TEST_MESSAGE("Finished Consuming");
        auto [consume_recorded_in, consume_recorded_eg]
          = tuple_binary_op<std::minus<>>{}(
            get_recorded_traffic(),
            std::make_tuple(init_recorded_in, init_recorded_eg));
        auto throttle_hist = get_throttle_time();

        // otherwise test is not valid:
        BOOST_REQUIRE_GT(kafka_in_data_len, kafka_eg_data_len);

        BOOST_CHECK_GT(produce_recorded_in, produce_recorded_eg);
        BOOST_CHECK_LT(consume_recorded_in, consume_recorded_eg);

        BOOST_CHECK_EQUAL(produce_recorded_in, kafka_in_total_len);
        BOOST_CHECK_EQUAL(consume_recorded_eg, kafka_eg_total_len);

        const auto time_estimated_in = std::chrono::milliseconds{
          kafka_in_total_len * 1000 / rate_limit_in};

        const auto time_estimated_eg = std::chrono::milliseconds{
          kafka_eg_total_len * 1000 / rate_limit_out};

        BOOST_TEST_CHECK(
          abs(duration_in - time_estimated_in)
            < time_estimated_in * tolerance_percent / 100,
          "Total ingress time: stop-start["
            << duration_in << "] ≈ time_estimated[" << time_estimated_in
            << "] ±" << tolerance_percent
            << "%, error: " << std::setprecision(3)
            << (duration_in - time_estimated_in) * 100.0 / time_estimated_in
            << "%");

        BOOST_TEST_CHECK(
          abs(duration_eg - time_estimated_eg)
            < time_estimated_eg * tolerance_percent / 100,
          "Total egress time: stop-start["
            << duration_eg << "] ≈ time_estimated[" << time_estimated_eg
            << "] ±" << tolerance_percent
            << "%, error: " << std::setprecision(3)
            << (duration_eg - time_estimated_eg) * 100.0 / time_estimated_eg
            << "%");

        constexpr auto get_throttle_percentile =
          [](const ss::metrics::histogram& h, int p) {
              auto count_p = double(h.sample_count) * p / 100;
              auto lb = std::ranges::lower_bound(
                h.buckets,
                count_p,
                std::less{},
                &ss::metrics::histogram_bucket::count);
              if (lb != h.buckets.begin()) {
                  auto prev = std::prev(lb);
                  double ratio = (count_p - prev->count)
                                 / (lb->count - prev->count);
                  return prev->upper_bound
                         + ratio * (lb->upper_bound - prev->upper_bound);
              }
              return lb->upper_bound;
          };

        const auto expected_bucket = std::ranges::lower_bound(
          throttle_hist.buckets,
          expected_max_throttle * 2, // add some leeway
          std::less{},
          &ss::metrics::histogram_bucket::upper_bound);

        auto throttle_90 = get_throttle_percentile(throttle_hist, 90);
        BOOST_TEST_CHECK(throttle_90 <= expected_bucket->upper_bound);
    }
};

FIXTURE_TEST(
  test_node_throughput_v2_limits_no_throttle_seq, throughput_limits_fixure) {
    test_throughput(honour_throttle::no, execution::seq);
}

FIXTURE_TEST(
  test_node_throughput_v2_limits_with_throttle_seq, throughput_limits_fixure) {
    test_throughput(honour_throttle::yes, execution::seq);
}

FIXTURE_TEST(
  test_node_throughput_v2_limits_no_throttle_par, throughput_limits_fixure) {
    test_throughput(honour_throttle::no, execution::par);
}

FIXTURE_TEST(
  test_node_throughput_v2_limits_with_throttle_par, throughput_limits_fixure) {
    test_throughput(honour_throttle::yes, execution::par);
}

// TODO: move producer utilities somewhere else and give this test a proper
// home.
FIXTURE_TEST(test_offset_for_leader_epoch, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    model::ntp ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));
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
                }).get();
            })
          .get();
        app.partition_manager
          .invoke_on(
            *shard,
            [this, ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                produce([this](size_t cnt) {
                    return small_batches(cnt);
                }).get();
            })
          .get();
    }
    // Prefix truncate the log so the beginning of the log moves forward.
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            auto partition = mgr.get(ntp);
            auto local_kafka_start_offset = partition->log()->from_log_offset(
              model::offset(1));
            partition
              ->prefix_truncate(
                model::offset(1),
                model::offset_cast(local_kafka_start_offset),
                model::no_timeout)
              .get();
        })
      .get();

    // Make a request getting the offset from a term below the start of the
    // log.
    auto& client = consumers.front();
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
    auto resp = client.dispatch(std::move(req), kafka::api_version(2)).get();
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
                auto start_offset = partition->log()->offsets().start_offset;
                return partition->log()->from_log_offset(start_offset);
            })
          .get();
    BOOST_REQUIRE_EQUAL(earliest_kafka_offset, partition_resp.end_offset);
}

FIXTURE_TEST(test_basic_delete_around_batch, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    const model::partition_id pid(0);
    const model::ntp ntp(test_tp_ns.ns, test_tp_ns.tp, pid);
    auto partition = app.partition_manager.local().get(ntp);
    auto log = partition->log();

    tests::kafka_produce_transport producer(make_kafka_client().get());
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });
    producer.start().get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key0", "val0"},
          {"key1", "val1"},
          {"key2", "val2"},
        })
      .get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key3", "val3"},
          {"key4", "val4"},
        })
      .get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key5", "val5"},
          {"key6", "val6"},
        })
      .get();
    log->flush().get();
    log->force_roll().get();
    BOOST_REQUIRE_EQUAL(2, log->segments().size());

    tests::kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto deferred_c_close = ss::defer([&consumer] { consumer.stop().get(); });
    tests::kafka_delete_records_transport deleter(make_kafka_client().get());
    auto deferred_d_close = ss::defer([&deleter] { deleter.stop().get(); });
    deleter.start().get();

    // At this point, we have three batches:
    //  [0, 2] [3, 4], [5, 6]
    const auto check_consume_out_of_range = [&](model::offset kafka_offset) {
        BOOST_REQUIRE_EXCEPTION(
          consumer.consume_from_partition(test_topic, pid, kafka_offset).get(),
          std::runtime_error,
          [](std::runtime_error e) {
              return std::string(e.what()).find("out_of_range")
                     != std::string::npos;
          });
    };
    {
        // Delete in the middle of an offset.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(1), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(1), lwm);
        // We should fail to consume below the start offset.
        check_consume_out_of_range(model::offset(0));
        // But the data may still be returned, and is expected to be filtered
        // client-side.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(1))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key0", consumed_records[0].key);
    }

    {
        // Delete at the start of a batch boundary.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(3), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(3), lwm);
        check_consume_out_of_range(model::offset(2));
        // No extraneous data should exist.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(3))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key3", consumed_records[0].key);
    }

    {
        // Delete near the end.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(6), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(6), lwm);
        check_consume_out_of_range(model::offset(5));
        // The entire batch is read.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(6))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key5", consumed_records[0].key);
    }
    auto lwm = deleter
                 .delete_records_from_partition(
                   test_topic, pid, model::offset(7), 5s)
                 .get();
    BOOST_REQUIRE_EQUAL(model::offset(7), lwm);
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key7", "val7"},
        })
      .get();
    log->flush().get();
    check_consume_out_of_range(model::offset(6));
    auto consumed_records
      = consumer.consume_from_partition(test_topic, pid, model::offset(7))
          .get();
    BOOST_REQUIRE(!consumed_records.empty());
    BOOST_REQUIRE_EQUAL("key7", consumed_records[0].key);
}

FIXTURE_TEST(test_produce_bad_timestamps, prod_consume_fixture) {
    /*
     * this tests produces messages with timestamps in the future and in the
     * past, and checks that the metric
     * vectorized_kafka_rpc_produces_with_timestamps_out_of_bounds is correctly
     * incremented.
     */

    wait_for_controller_leadership().get();
    start();
    auto ntp = model::ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    // helper to produce a bunch of messages with some drift applied to the
    // timestamps. the drift is the same for all the messages, but a more
    // advaced test would be to have a range of drifts from start to finish
    auto produce_messages = [&](std::chrono::system_clock::duration drift) {
        try {
            producer
              .produce_to_partition(
                ntp.tp.topic,
                ntp.tp.partition,
                {
                  {"key0", "val0"},
                  {"key1", "val1"},
                  {"key2", "val2"},
                },
                model::to_timestamp(std::chrono::system_clock::now() + drift))
              .get();
        } catch (...) {
            // Fall through
        }
    };

    BOOST_TEST_INFO("expect produce_bad_create_time to be 0");
    auto bad_timestamps_metric = produce_bad_ts_count();
    BOOST_CHECK_EQUAL(0, bad_timestamps_metric);

    BOOST_TEST_INFO("messages with no skew do not trigger the probe");
    produce_messages(0s);
    BOOST_CHECK_EQUAL(0, produce_bad_ts_count());

    BOOST_TEST_INFO(
      "messages with a skew towards the future trigger the probe");
    config::shard_local_cfg().log_message_timestamp_after_max_ms.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    produce_messages(2h);
    BOOST_CHECK_LT(bad_timestamps_metric, produce_bad_ts_count());

    bad_timestamps_metric = produce_bad_ts_count();

    BOOST_TEST_INFO("messages with a skew towards the past trigger the probe");
    config::shard_local_cfg().log_message_timestamp_before_max_ms.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    produce_messages(-2h);
    BOOST_CHECK_LT(bad_timestamps_metric, produce_bad_ts_count());

    bad_timestamps_metric = produce_bad_ts_count();

    BOOST_TEST_INFO("messages within the bounds to not trigger the probe");
    produce_messages(-30min);
    produce_messages(30min);
    BOOST_CHECK_EQUAL(bad_timestamps_metric, produce_bad_ts_count());

    BOOST_TEST_INFO(
      "disabling the alert for the past allows messages in the "
      "past without triggering the probe");
    config::shard_local_cfg().log_message_timestamp_before_max_ms.set_value(
      std::chrono::milliseconds::max());
    produce_messages(-365 * 24h);
    BOOST_CHECK_EQUAL(bad_timestamps_metric, produce_bad_ts_count());
}

FIXTURE_TEST(test_compression_metrics, prod_consume_fixture) {
    using ctype = model::compression;

    wait_for_controller_leadership().get();
    start();
    auto ntp = model::ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    auto produce_messages = [&](ctype compression) {
        producer
          .produce_to_partition(
            ntp.tp.topic,
            ntp.tp.partition,
            {{"key0", "val0"}},
            model::timestamp::now(),
            compression)
          .get();
    };

    for (auto c : model::all_batch_compression_types) {
        BOOST_TEST_INFO("initially all bytes zero for " << c);
        BOOST_CHECK_EQUAL(0, bytes_by_compression(c));
    }

    // this compression type and those greater are expected to have zero bytes
    // produced, but lower ones should have non-zero bytes produced
    for (ctype last_nonzero : model::all_batch_compression_types) {
        produce_messages(last_nonzero);
        for (auto ctype : model::all_batch_compression_types) {
            if (ctype <= last_nonzero) {
                BOOST_TEST_INFO(
                  "testing non-zero bytes in metric for " << ctype);
                BOOST_CHECK_GT(bytes_by_compression(ctype), 0);
            } else {
                BOOST_TEST_INFO("testing zero bytes in metric for " << ctype);
                BOOST_CHECK_EQUAL(0, bytes_by_compression(ctype));
            }
        }
    }
}

FIXTURE_TEST(test_produce_unset_max_timestamp_legacy, prod_consume_fixture) {
    scoped_config cfg;
    cfg.get("kafka_produce_batch_validation")
      .set_value(model::kafka_batch_validation_mode::legacy);
    wait_for_controller_leadership().get();
    start();
    auto ntp = model::ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_p_close = ss::defer([&producer] { producer.stop().get(); });

    // Produce a batch per compression type with 3 records and `max_timestamp`
    // left unset.
    auto produce_messages = [&](model::compression ctype) {
        static std::vector<kv_t> records = {
          {"key0", "val0"},
          {"key1", "val1"},
          {"key2", "val2"},
        };
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.set_compression(ctype);
        for (auto& kv : records) {
            const auto& k = kv.key;
            const auto& v_opt = kv.val;
            iobuf key_buf;
            key_buf.append(k.data(), k.size());
            std::optional<iobuf> val_buf;
            if (v_opt.has_value()) {
                const auto& v = v_opt.value();
                val_buf = iobuf::from({v.data(), v.size()});
            }
            builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
        }
        builder.set_timestamp(model::timestamp::now());
        auto batch = std::move(builder).build();

        // Override max timestamp with `missing`
        batch.set_max_timestamp(
          model::timestamp_type::create_time, model::timestamp::missing());
        producer
          .produce_to_partition(
            ntp.tp.topic, ntp.tp.partition, std::move(batch))
          .get();
    };

    for (auto c : model::all_batch_compression_types) {
        produce_messages(c);
    }

    auto transport = make_kafka_client().get();
    transport.connect().get();
    auto deferred_t_close = ss::defer([&transport] { transport.stop().get(); });

    // Perform a manual fetch in order to consume the produced batch, not
    // individual records.
    std::vector<model::record_batch> batches;
    {
        kafka::fetch_request::topic topic;
        topic.topic = ntp.tp.topic;
        kafka::fetch_request::partition partition{
          .partition = ntp.tp.partition,
          .fetch_offset = model::offset{0},
          .log_start_offset = model::offset{0},
          .partition_max_bytes = 100_MiB};
        topic.partitions.emplace_back(std::move(partition));
        kafka::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 100_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));
        auto fetch_resp
          = transport.dispatch(std::move(req), kafka::api_version(4)).get();
        BOOST_REQUIRE_EQUAL(
          fetch_resp.data.error_code, kafka::error_code::none);
        auto& data = fetch_resp.data;
        for (auto& topic : data.responses) {
            for (auto& partition : topic.partitions) {
                BOOST_REQUIRE_EQUAL(
                  partition.error_code, kafka::error_code::none);
                BOOST_REQUIRE(partition.records.has_value());
                while (!partition.records->is_end_of_stream()) {
                    auto batch_adapter
                      = partition.records.value().consume_batch();
                    BOOST_REQUIRE(batch_adapter.batch.has_value());
                    batches.push_back(std::move(batch_adapter.batch).value());
                }
            }
        }
    }

    // Check that the `max_timestamp` was left as missing (== {-1}) for batches
    // with compression in the produce path, and set (!= {-1}) for batches
    // without compression when left unset by client in `legacy` validation
    // mode.
    for (const auto& batch : batches) {
        if (batch.compressed()) {
            BOOST_REQUIRE_EQUAL(
              batch.header().max_timestamp, model::timestamp::missing());
        } else {
            BOOST_REQUIRE_NE(
              batch.header().max_timestamp, model::timestamp::missing());
        }
    }
}

FIXTURE_TEST(test_produce_unset_max_timestamp_relaxed, prod_consume_fixture) {
    scoped_config cfg;
    cfg.get("kafka_produce_batch_validation")
      .set_value(model::kafka_batch_validation_mode::relaxed);
    wait_for_controller_leadership().get();
    start();
    auto ntp = model::ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_p_close = ss::defer([&producer] { producer.stop().get(); });

    // Produce a batch per compression type with 3 records and `max_timestamp`
    // left unset.
    auto produce_messages = [&](model::compression ctype) {
        static std::vector<kv_t> records = {
          {"key0", "val0"},
          {"key1", "val1"},
          {"key2", "val2"},
        };
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.set_compression(ctype);
        for (auto& kv : records) {
            const auto& k = kv.key;
            const auto& v_opt = kv.val;
            iobuf key_buf;
            key_buf.append(k.data(), k.size());
            std::optional<iobuf> val_buf;
            if (v_opt.has_value()) {
                const auto& v = v_opt.value();
                val_buf = iobuf::from({v.data(), v.size()});
            }
            builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
        }
        builder.set_timestamp(model::timestamp::now());
        auto batch = std::move(builder).build();

        // Override max timestamp with `missing`
        batch.set_max_timestamp(
          model::timestamp_type::create_time, model::timestamp::missing());
        producer
          .produce_to_partition(
            ntp.tp.topic, ntp.tp.partition, std::move(batch))
          .get();
    };

    for (auto c : model::all_batch_compression_types) {
        produce_messages(c);
    }

    auto transport = make_kafka_client().get();
    transport.connect().get();
    auto deferred_t_close = ss::defer([&transport] { transport.stop().get(); });

    // Perform a manual fetch in order to consume the produced batch, not
    // individual records.
    std::vector<model::record_batch> batches;
    {
        kafka::fetch_request::topic topic;
        topic.topic = ntp.tp.topic;
        kafka::fetch_request::partition partition{
          .partition = ntp.tp.partition,
          .fetch_offset = model::offset{0},
          .log_start_offset = model::offset{0},
          .partition_max_bytes = 100_MiB};
        topic.partitions.emplace_back(std::move(partition));
        kafka::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 100_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));
        auto fetch_resp
          = transport.dispatch(std::move(req), kafka::api_version(4)).get();
        BOOST_REQUIRE_EQUAL(
          fetch_resp.data.error_code, kafka::error_code::none);
        auto& data = fetch_resp.data;
        for (auto& topic : data.responses) {
            for (auto& partition : topic.partitions) {
                BOOST_REQUIRE_EQUAL(
                  partition.error_code, kafka::error_code::none);
                BOOST_REQUIRE(partition.records.has_value());
                while (!partition.records->is_end_of_stream()) {
                    auto batch_adapter
                      = partition.records.value().consume_batch();
                    BOOST_REQUIRE(batch_adapter.batch.has_value());
                    batches.push_back(std::move(batch_adapter.batch).value());
                }
            }
        }
    }

    // Check that the `max_timestamp` was indeed set (!= {-1}) in the produce
    // path when left unset by client in `relaxed` validation mode.
    for (const auto& batch : batches) {
        BOOST_REQUIRE_NE(
          batch.header().max_timestamp, model::timestamp::missing());
    }
}

// Test that the log_append_time_ms field in the produce response is set
// correctly based on the topic's timestamp type:
// - CreateTime: log_append_time_ms should be -1 (missing)
// - LogAppendTime: log_append_time_ms should be the broker's local timestamp
FIXTURE_TEST(test_produce_log_append_time_response, prod_consume_fixture) {
    wait_for_controller_leadership().get();

    // Create topic with CreateTime
    const model::topic_namespace create_time_tp_ns{
      model::kafka_namespace, model::topic{"create-time-topic"}};
    cluster::topic_properties create_time_props;
    create_time_props.timestamp_type = model::timestamp_type::create_time;
    add_topic(create_time_tp_ns, 1, create_time_props).get();

    // Create topic with LogAppendTime
    const model::topic_namespace log_append_time_tp_ns{
      model::kafka_namespace, model::topic{"log-append-time-topic"}};
    cluster::topic_properties log_append_props;
    log_append_props.timestamp_type = model::timestamp_type::append_time;
    add_topic(log_append_time_tp_ns, 1, log_append_props).get();

    // Wait for leadership on both partitions
    model::ntp create_time_ntp(
      create_time_tp_ns.ns, create_time_tp_ns.tp, model::partition_id(0));
    model::ntp log_append_time_ntp(
      log_append_time_tp_ns.ns,
      log_append_time_tp_ns.tp,
      model::partition_id(0));
    wait_for_leader(create_time_ntp).get();
    wait_for_leader(log_append_time_ntp).get();

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    auto transport = make_kafka_client().get();
    transport.connect().get();
    auto deferred_t_close = ss::defer([&transport] { transport.stop().get(); });

    // Helper to build and send a produce request for a given topic
    auto produce_to_topic =
      [&](const model::topic& topic) -> ss::future<kafka::produce_response> {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.add_raw_kv(iobuf::from("key"), iobuf::from("value"));

        kafka::produce_request::partition partition;
        partition.partition_index = model::partition_id(0);
        partition.records.emplace(std::move(builder).build());

        chunked_vector<kafka::produce_request::partition> partitions;
        partitions.push_back(std::move(partition));

        kafka::produce_request::topic tp;
        tp.name = topic;
        tp.partitions = std::move(partitions);

        chunked_vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));

        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return transport.dispatch(std::move(req), kafka::api_version(7));
    };

    // Produce to CreateTime topic
    auto create_time_resp = produce_to_topic(create_time_tp_ns.tp).get();
    BOOST_REQUIRE_EQUAL(create_time_resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      create_time_resp.data.responses[0].partitions.size(), 1);
    const auto& create_time_partition
      = create_time_resp.data.responses[0].partitions[0];
    BOOST_REQUIRE_EQUAL(
      create_time_partition.error_code, kafka::error_code::none);

    // For CreateTime topics, log_append_time_ms should be -1 (missing)
    BOOST_CHECK_EQUAL(
      create_time_partition.log_append_time_ms, model::timestamp::missing());

    // Produce to LogAppendTime topic
    auto log_append_time_resp
      = produce_to_topic(log_append_time_tp_ns.tp).get();
    BOOST_REQUIRE_EQUAL(log_append_time_resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      log_append_time_resp.data.responses[0].partitions.size(), 1);
    const auto& log_append_time_partition
      = log_append_time_resp.data.responses[0].partitions[0];
    BOOST_REQUIRE_EQUAL(
      log_append_time_partition.error_code, kafka::error_code::none);

    // For LogAppendTime topics, log_append_time_ms should be a valid timestamp
    BOOST_CHECK_NE(
      log_append_time_partition.log_append_time_ms,
      model::timestamp::missing());
    // Also verify it's a reasonable timestamp (within the last minute)
    auto now = model::timestamp::now();
    auto one_minute_ago = model::timestamp(now.value() - 60000);
    BOOST_CHECK_GE(
      log_append_time_partition.log_append_time_ms, one_minute_ago);
    BOOST_CHECK_LE(log_append_time_partition.log_append_time_ms, now);
}

FIXTURE_TEST(test_produce_unset_timestamps_relaxed, prod_consume_fixture) {
    scoped_config cfg;
    cfg.get("kafka_produce_batch_validation")
      .set_value(model::kafka_batch_validation_mode::relaxed);
    wait_for_controller_leadership().get();
    start();
    auto ntp = model::ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));

    auto producer = tests::kafka_produce_transport(make_kafka_client().get());
    producer.start().get();
    auto deferred_p_close = ss::defer([&producer] { producer.stop().get(); });

    // Produce a batch per compression type with 3 records and `max_timestamp`
    // left unset.
    auto produce_messages = [&](model::compression ctype) {
        static std::vector<kv_t> records = {
          {"key0", "val0"},
          {"key1", "val1"},
          {"key2", "val2"},
        };
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.set_compression(ctype);
        for (auto& kv : records) {
            const auto& k = kv.key;
            const auto& v_opt = kv.val;
            iobuf key_buf;
            key_buf.append(k.data(), k.size());
            std::optional<iobuf> val_buf;
            if (v_opt.has_value()) {
                const auto& v = v_opt.value();
                val_buf = iobuf::from({v.data(), v.size()});
            }
            builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
        }
        builder.set_timestamp(model::timestamp::missing());
        auto batch = std::move(builder).build();

        // Override max timestamp with `missing`
        batch.set_max_timestamp(
          model::timestamp_type::create_time, model::timestamp::missing());
        producer
          .produce_to_partition(
            ntp.tp.topic, ntp.tp.partition, std::move(batch))
          .get();
    };

    for (auto c : model::all_batch_compression_types) {
        produce_messages(c);
    }

    auto transport = make_kafka_client().get();
    transport.connect().get();
    auto deferred_t_close = ss::defer([&transport] { transport.stop().get(); });

    // Perform a manual fetch in order to consume the produced batch, not
    // individual records.
    std::vector<model::record_batch> batches;
    {
        kafka::fetch_request::topic topic;
        topic.topic = ntp.tp.topic;
        kafka::fetch_request::partition partition{
          .partition = ntp.tp.partition,
          .fetch_offset = model::offset{0},
          .log_start_offset = model::offset{0},
          .partition_max_bytes = 100_MiB};
        topic.partitions.emplace_back(std::move(partition));
        kafka::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 100_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));
        auto fetch_resp
          = transport.dispatch(std::move(req), kafka::api_version(4)).get();
        BOOST_REQUIRE_EQUAL(
          fetch_resp.data.error_code, kafka::error_code::none);
        auto& data = fetch_resp.data;
        for (auto& topic : data.responses) {
            for (auto& partition : topic.partitions) {
                BOOST_REQUIRE_EQUAL(
                  partition.error_code, kafka::error_code::none);
                BOOST_REQUIRE(partition.records.has_value());
                while (!partition.records->is_end_of_stream()) {
                    auto batch_adapter
                      = partition.records.value().consume_batch();
                    BOOST_REQUIRE(batch_adapter.batch.has_value());
                    batches.push_back(std::move(batch_adapter.batch).value());
                }
            }
        }
    }

    // Check that the `max_timestamp` was left (== {-1}) in the produce
    // path when left unset by client in `relaxed` validation mode, since all
    // records also had {-1} as their timestamp.
    for (const auto& batch : batches) {
        BOOST_REQUIRE_EQUAL(
          batch.header().max_timestamp, model::timestamp::missing());
    }
}
