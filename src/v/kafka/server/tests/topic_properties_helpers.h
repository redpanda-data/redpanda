// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/config_frontend.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/create_partitions.h"
#include "kafka/protocol/create_topics.h"
#include "redpanda/tests/fixture.h"

class topic_properties_test_fixture : public redpanda_thread_fixture {
public:
    topic_properties_test_fixture() { wait_for_controller_leadership().get(); }

    void revoke_license() {
        app.controller->get_feature_table()
          .invoke_on_all([](auto& ft) { return ft.revoke_license(); })
          .get();
    }

    void reinstall_license() {
        app.controller->get_feature_table()
          .invoke_on_all([](auto& ft) {
              return ft.set_builtin_trial_license(model::timestamp::now());
          })
          .get();
    }

    void update_cluster_config(std::string_view k, std::string_view v) {
        app.controller->get_config_frontend()
          .local()
          .patch(
            cluster::config_update_request{
              .upsert{{ss::sstring{k}, ss::sstring{v}}},
            },
            model::timeout_clock::now() + 5s)
          .get();
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

    kafka::create_topics_response create_topic(
      const model::topic& tp,
      const absl::flat_hash_map<ss::sstring, ss::sstring>& properties,
      int num_partitions = 1,
      int16_t replication_factor = 1) {
        kafka::creatable_topic topic{
          .name = model::topic(tp),
          .num_partitions = num_partitions,
          .replication_factor = replication_factor,
        };
        for (auto& [k, v] : properties) {
            kafka::createable_topic_config config;
            config.name = k;
            config.value = v;
            topic.configs.push_back(std::move(config));
        }

        auto req = kafka::create_topics_request{.data{
          .topics = {topic},
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        return do_with_client([req = std::move(req)](
                                kafka::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), kafka::api_version(0));
               })
          .get();
    }

    kafka::create_partitions_response
    create_partitions(const model::topic& tp, int32_t count) {
        return do_with_client(
                 [tp, count](kafka::client::transport& client) mutable {
                     chunked_vector<kafka::create_partitions_topic> topics;
                     topics.emplace_back(kafka::create_partitions_topic{
                       .name = tp,
                       .count = count,
                       .assignments = std::nullopt,
                       .unknown_tags = {}});
                     return client.dispatch(
                       kafka::create_partitions_request{
                         .data{
                           .topics = std::move(topics),
                           .timeout_ms = 10s,
                           .validate_only = false,
                           .unknown_tags = {}},

                       },
                       kafka::api_version{3});
                 })
          .get();
    }
};
