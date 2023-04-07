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

    template<typename T>
    ss::future<model::offset> produce(T&& batch_factory) {
        kafka::produce_request::topic tp;
        size_t count = random_generators::get_int(1, 20);
        tp.partitions = batch_factory(count);
        tp.name = test_topic;
        std::vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer->dispatch(std::move(req))
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
          kafka::produce_request(std::nullopt, 1, std::move(topics)),
          unsupported_version)
        .get(),
      kafka::client::kafka_request_disconnected_exception);
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
