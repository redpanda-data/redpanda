// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/fragmented_vector.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/delete_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/produce.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <optional>
#include <vector>

using namespace std::chrono_literals; // NOLINT

class recreate_test_fixture : public redpanda_thread_fixture {
public:
    void create_topic(ss::sstring tp, int32_t partitions, int16_t rf) {
        kafka::creatable_topic topic{
          .name = model::topic(tp),
          .num_partitions = partitions,
          .replication_factor = rf,
        };

        auto req = kafka::create_topics_request{.data{
          .topics = {topic},
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        auto client = make_kafka_client().get();
        client.connect().get();
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(2)).get();
    }
    kafka::delete_topics_request make_delete_topics_request(
      chunked_vector<model::topic> topics, std::chrono::milliseconds timeout) {
        kafka::delete_topics_request req;
        req.data.topic_names = std::move(topics);
        req.data.timeout_ms = timeout;
        return req;
    }

    kafka::delete_topics_response
    delete_topics(std::vector<model::topic> topics) {
        return send_delete_topics_request(make_delete_topics_request(
          chunked_vector<model::topic>{
            std::make_move_iterator(topics.begin()),
            std::make_move_iterator(topics.end())},
          5s));
    }

    kafka::delete_topics_response
    send_delete_topics_request(kafka::delete_topics_request req) {
        auto client = make_kafka_client().get();
        client.connect().get();

        return client.dispatch(std::move(req), kafka::api_version(2)).get();
    }
    template<typename Func>
    auto do_with_client(Func&& f) {
        return make_kafka_client().then(
          [f = std::forward<Func>(f)](kafka::client::transport client) mutable {
              return ss::do_with(
                std::move(client),
                [f = std::forward<Func>(f)](
                  kafka::client::transport& client) mutable {
                    return client.connect().then(
                      [&client, f = std::forward<Func>(f)]() mutable {
                          return f(client);
                      });
                });
          });
    }

    ss::future<kafka::metadata_response>
    get_topic_metadata(const model::topic& tp) {
        return do_with_client([tp](kafka::client::transport& client) {
            chunked_vector<kafka::metadata_request_topic> topics;
            topics.push_back(kafka::metadata_request_topic{tp});
            kafka::metadata_request md_req{
              .data
              = {.topics = std::make_optional(std::move(topics)), .allow_auto_topic_creation = false},
              .list_all_topics = false};
            return client.dispatch(std::move(md_req));
        });
    }

    ss::future<>
    wait_until_topic_status(const model::topic& tp, kafka::error_code ec) {
        return tests::cooperative_spin_wait_with_timeout(3s, [this, tp, ec] {
            return get_topic_metadata(tp).then(
              [ec](kafka::metadata_response md) {
                  if (md.data.topics.empty()) {
                      return false;
                  }
                  return md.data.topics.begin()->error_code == ec;
              });
        });
    }

    void restart() {
        shutdown();
        app_signal = std::make_unique<::stop_signal>();
        ss::smp::invoke_on_all([] {
            auto& config = config::shard_local_cfg();
            config.get("disable_metrics").set_value(false);
        }).get();
        app.initialize(proxy_config(), proxy_client_config());
        app.check_environment();
        app.wire_up_and_start(*app_signal, true);
    }
};

FIXTURE_TEST(test_topic_recreation, recreate_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp(), 6, 1);
    // wait until created
    wait_until_topic_status(test_tp, kafka::error_code::none).get();

    delete_topics({test_tp});
    // wait until deleted
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get();
    create_topic(test_tp(), 6, 1);

    tests::cooperative_spin_wait_with_timeout(3s, [this, test_tp] {
        return ss::async([this, test_tp] {
            auto md = get_topic_metadata(test_tp).get();
            if (md.data.topics.size() != 1) {
                return false;
            }
            auto& partitions = md.data.topics.begin()->partitions;
            if (partitions.size() != 6) {
                return false;
            }

            return std::all_of(
              partitions.begin(),
              partitions.end(),
              [](kafka::metadata_response::partition& p) {
                  return p.leader_id == model::node_id{1};
              });
        });
    }).get();
}

FIXTURE_TEST(test_topic_recreation_recovery, recreate_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    // flow frim [ch1061]
    info("Creating {} with {} partitions", test_tp, 6);
    create_topic(test_tp(), 6, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get();
    info("Deleting {}", test_tp);
    delete_topics({test_tp});
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get();
    info("Restarting redpanda, first time");
    restart();
    wait_for_controller_leadership().get();
    info("Creating {} with {} partitions", test_tp, 3);
    create_topic(test_tp(), 3, 1);
    info("Deleting {}", test_tp);
    delete_topics({test_tp});
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get();
    info("Creating {} with {} partitions", test_tp, 3);
    create_topic(test_tp(), 3, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get();
    info("Restarting redpanda, second time");
    restart();
    info("Waiting for recovery");
    wait_for_controller_leadership().get();
    wait_until_topic_status(test_tp, kafka::error_code::none).get();

    tests::cooperative_spin_wait_with_timeout(5s, [test_tp, this] {
        return get_topic_metadata(test_tp).then(
          [](kafka::metadata_response md) {
              auto& partitions = md.data.topics.begin()->partitions;
              if (partitions.size() != 3) {
                  return false;
              }
              return std::all_of(
                partitions.begin(),
                partitions.end(),
                [](kafka::metadata_response::partition& p) {
                    return p.leader_id == model::node_id{1};
                });
          });
    }).get();
}

FIXTURE_TEST(test_recreated_topic_does_not_lose_data, recreate_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    // flow from [ch1406]
    info("Creating {} with {} partitions", test_tp, 1);
    create_topic(test_tp(), 1, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get();
    info("Deleting {}", test_tp);
    delete_topics({test_tp});
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get();
    info("Creating {} with {} partitions", test_tp, 1);
    create_topic(test_tp(), 1, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get();
    auto ntp = model::ntp(
      model::kafka_namespace,
      model::topic_partition(test_tp, model::partition_id(0)));

    auto wait_for_ntp_leader = [this, ntp] {
        auto shard_id = app.shard_table.local().shard_for(ntp);
        if (!shard_id) {
            return ss::make_ready_future<bool>(false);
        }
        return app.partition_manager.invoke_on(
          *shard_id, [ntp](cluster::partition_manager& pm) {
              if (pm.get(ntp)) {
                  return pm.get(ntp)->is_elected_leader();
              }
              return false;
          });
    };
    tests::cooperative_spin_wait_with_timeout(2s, wait_for_ntp_leader).get();
    auto shard_id = app.shard_table.local().shard_for(ntp);
    model::offset committed_offset
      = app.partition_manager
          .invoke_on(
            *shard_id,
            [ntp](cluster::partition_manager& pm) {
                return model::test::make_random_batches(model::offset(0), 5)
                  .then([&pm, ntp](auto batches) {
                      auto rdr = model::make_memory_record_batch_reader(
                        std::move(batches));
                      auto p = pm.get(ntp);
                      return p->raft()
                        ->replicate(
                          std::move(rdr),
                          raft::replicate_options(
                            raft::consistency_level::quorum_ack))
                        .then([p](auto) { return p->committed_offset(); });
                  });
            })
          .get();
    info("Restarting redpanda");
    restart();

    // make sure we can read the same amount of data
    {
        info("Expected committed offset {}", committed_offset);
        wait_for_controller_leadership().get();
        tests::cooperative_spin_wait_with_timeout(2s, wait_for_ntp_leader)
          .get();
        tests::cooperative_spin_wait_with_timeout(2s, [this, ntp] {
            auto shard_id = app.shard_table.local().shard_for(ntp);
            if (!shard_id) {
                return ss::make_ready_future<bool>(false);
            }

            return app.partition_manager.invoke_on(
              *shard_id, [ntp](cluster::partition_manager& pm) {
                  auto partition = pm.get(ntp);
                  return partition
                         && partition->committed_offset() >= model::offset(0);
              });
        }).get();
        auto shard_id = app.shard_table.local().shard_for(ntp);
        app.partition_manager
          .invoke_on(
            *shard_id,
            [ntp, committed_offset](cluster::partition_manager& pm) {
                BOOST_REQUIRE(
                  pm.get(ntp)->committed_offset() >= committed_offset);
            })
          .get();
    }
}
