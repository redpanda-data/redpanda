#include "kafka/errors.h"
#include "kafka/requests/batch_consumer.h"
#include "kafka/requests/delete_topics_request.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/produce_request.h"
#include "kafka/requests/topics/types.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "storage/tests/utils/random_batch.h"
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

        std::vector<kafka::creatable_topic> topics;
        topics.push_back(std::move(topic));
        auto req = kafka::create_topics_request{.data{
          .topics = std::move(topics),
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        auto client = make_kafka_client().get0();
        client.connect().get0();
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(2)).get0();
    }
    kafka::delete_topics_request make_delete_topics_request(
      std::vector<model::topic> topics, std::chrono::milliseconds timeout) {
        kafka::delete_topics_request req;
        req.data.topic_names = std::move(topics);
        req.data.timeout_ms = timeout;
        return req;
    }

    kafka::delete_topics_response
    delete_topics(std::vector<model::topic> topics) {
        return send_delete_topics_request(
          make_delete_topics_request(std::move(topics), 5s));
    }

    kafka::delete_topics_response
    send_delete_topics_request(kafka::delete_topics_request req) {
        auto client = make_kafka_client().get0();
        client.connect().get0();

        return client.dispatch(std::move(req), kafka::api_version(2)).get0();
    }
    template<typename Func>
    auto do_with_client(Func&& f) {
        return make_kafka_client().then(
          [f = std::forward<Func>(f)](kafka::client client) mutable {
              return ss::do_with(
                std::move(client),
                [f = std::forward<Func>(f)](kafka::client& client) mutable {
                    return client.connect().then(
                      [&client, f = std::forward<Func>(f)]() mutable {
                          return f(client);
                      });
                });
          });
    }

    ss::future<kafka::metadata_response>
    get_topic_metadata(const model::topic& tp) {
        return do_with_client([tp](kafka::client& client) {
            std::vector<model::topic> topics;
            topics.push_back(tp);
            kafka::metadata_request md_req{
              .topics = topics,
              .allow_auto_topic_creation = false,
              .list_all_topics = false};
            return client.dispatch(md_req);
        });
    }

    ss::future<>
    wait_until_topic_status(const model::topic& tp, kafka::error_code ec) {
        return tests::cooperative_spin_wait_with_timeout(3s, [this, tp, ec] {
            return get_topic_metadata(tp).then(
              [ec](kafka::metadata_response md) {
                  if (md.topics.empty()) {
                      return false;
                  }
                  return md.topics.begin()->err_code == ec;
              });
        });
    }

    void restart() {
        app.shutdown();
        ss::smp::invoke_on_all([this] {
            auto& config = config::shard_local_cfg();
            config.get("disable_metrics").set_value(false);
        }).get0();
        app.initialize();
        app.check_environment();
        app.configure_admin_server();
        app.wire_up_services();
        app.start();
    }
};

FIXTURE_TEST(test_topic_recreation, recreate_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp(), 6, 1);
    // wait until created
    wait_until_topic_status(test_tp, kafka::error_code::none).get0();

    delete_topics({test_tp});
    // wait until deleted
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get0();
    create_topic(test_tp(), 6, 1);

    tests::cooperative_spin_wait_with_timeout(3s, [this, test_tp] {
        return ss::async([this, test_tp] {
            auto md = get_topic_metadata(test_tp).get0();
            if (md.topics.size() != 1) {
                return false;
            }
            auto& partitions = md.topics.begin()->partitions;
            return std::all_of(
              partitions.begin(),
              partitions.end(),
              [](kafka::metadata_response::partition& p) {
                  return p.leader == model::node_id{1};
              });
        });
    }).get0();

    auto md = get_topic_metadata(test_tp).get0();
    BOOST_REQUIRE_EQUAL(md.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(md.topics.begin()->partitions.size(), 6);

    for (auto& p : md.topics.begin()->partitions) {
        BOOST_REQUIRE_EQUAL(p.leader, model::node_id{1});
    }
}

FIXTURE_TEST(test_topic_recreation_recovery, recreate_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    // flow frim [ch1061]
    info("Creating {} with {} partitions", test_tp, 6);
    create_topic(test_tp(), 6, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get0();
    info("Deleting {}", test_tp);
    delete_topics({test_tp});
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get0();
    info("Restarting redpanda, first time");
    restart();
    wait_for_controller_leadership().get();
    info("Creating {} with {} partitions", test_tp, 3);
    create_topic(test_tp(), 3, 1);
    info("Deleting {}", test_tp);
    delete_topics({test_tp});
    wait_until_topic_status(
      test_tp, kafka::error_code::unknown_topic_or_partition)
      .get0();
    info("Creating {} with {} partitions", test_tp, 3);
    create_topic(test_tp(), 3, 1);
    wait_until_topic_status(test_tp, kafka::error_code::none).get0();
    info("Restarting redpanda, second time");
    restart();
    info("Waiting for recovery");
    wait_for_controller_leadership().get();
    wait_until_topic_status(test_tp, kafka::error_code::none).get0();

    return tests::cooperative_spin_wait_with_timeout(
             5s,
             [test_tp, this] {
                 return get_topic_metadata(test_tp).then(
                   [](kafka::metadata_response md) {
                       auto& partitions = md.topics.begin()->partitions;
                       if (partitions.size() != 3) {
                           return false;
                       }
                       return std::all_of(
                         partitions.begin(),
                         partitions.end(),
                         [](kafka::metadata_response::partition& p) {
                             return p.leader == model::node_id{1};
                         });
                   });
             })
      .get0();
}
