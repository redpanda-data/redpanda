// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/security_frontend.h"
#include "container/chunked_vector.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/handlers/offset_fetch.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"
#include "utils/base64.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace kafka;
join_group_request make_join_group_request(
  ss::sstring member_id,
  ss::sstring gr,
  std::vector<ss::sstring> protocols,
  ss::sstring protocol_type) {
    join_group_request req;
    req.data.group_id = kafka::group_id(std::move(gr));
    req.data.member_id = kafka::member_id(std::move(member_id));
    req.data.protocol_type = kafka::protocol_type(std::move(protocol_type));
    for (auto& p : protocols) {
        req.data.protocols.push_back(
          join_group_request_protocol{
            .name = protocol_name(std::move(p)), .metadata = bytes{}});
    }
    req.data.session_timeout_ms = 10s;
    return req;
}
struct consumer_offsets_fixture : public redpanda_thread_fixture {
    void
    wait_for_consumer_offsets_topic(const kafka::group_instance_id& group) {
        auto client = make_kafka_client().get();

        client.connect().get();
        kafka::find_coordinator_request req(group);
        req.data.key_type = kafka::coordinator_type::group;
        client.dispatch(std::move(req), kafka::api_version(1)).get();

        app.controller->get_api()
          .local()
          .wait_for_topic(
            model::kafka_consumer_offsets_nt, model::timeout_clock::now() + 30s)
          .get();

        tests::cooperative_spin_wait_with_timeout(30s, [&group, &client] {
            kafka::describe_groups_request req;
            req.data.groups.emplace_back(group);
            return client.dispatch(std::move(req), kafka::api_version(1))
              .then([](kafka::describe_groups_response response) {
                  return response.data.groups.front().error_code
                         == kafka::error_code::none;
              });
        }).get();

        client.stop().get();
        client.shutdown();
    }

    void create_user(const ss::sstring& user, const ss::sstring& pass) {
        auto creds = security::scram_sha256::make_credentials(
          pass, security::scram_sha256::min_iterations);
        app.controller->get_security_frontend()
          .local()
          .create_user(
            security::credential_user(user),
            std::move(creds),
            model::timeout_clock::now() + 5s)
          .get();
    }

    template<typename T>
    void authorize(
      const ss::sstring& user,
      security::acl_operation op,
      const std::vector<T>& resources) {
        const auto make_binding = [&user, op](const T& resource) {
            return security::acl_binding{
              security::resource_pattern{
                security::get_resource_type<T>(),
                resource,
                security::pattern_type::literal},
              security::acl_entry{
                security::acl_principal{security::principal_type::user, user},
                security::acl_host::wildcard_host(),
                op,
                security::acl_permission::allow}};
        };

        app.controller->get_security_frontend()
          .local()
          .create_acls(
            resources | std::views::transform(make_binding)
              | std::ranges::to<std::vector>(),
            5s)
          .get();
    }
};

FIXTURE_TEST(join_empty_group_static_member, consumer_offsets_fixture) {
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    tests::cooperative_spin_wait_with_timeout(30s, [&gr, &client] {
        auto req = make_join_group_request(
          unknown_member_id, "group-test", {"p1", "p2"}, "random");
        // set group instance id
        req.data.group_instance_id = gr;
        return client.dispatch(std::move(req), kafka::api_version(5))
          .then([&](auto resp) {
              BOOST_REQUIRE(
                resp.data.error_code == kafka::error_code::none
                || resp.data.error_code == kafka::error_code::not_coordinator);
              return resp.data.error_code == kafka::error_code::none
                     && resp.data.member_id != unknown_member_id;
          });
    }).get();
}

FIXTURE_TEST(empty_offset_commit_request, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    add_topic(
      model::topic_namespace_view{model::kafka_namespace, model::topic{"foo"}})
      .get();
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();
    // Regression test for a crash that we would previously see with empty
    // requests.
    // NOTE: errors are stored in partition fields of the response. Since the
    // requests are empty, there are no errors.
    {
        auto req = offset_commit_request{
          .data{.group_id = kafka::group_id{"foo-topics"}, .topics = {}}};
        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
    {
        offset_commit_request req;
        req.data = offset_commit_request_data{
          .group_id = kafka::group_id{"foo-partitions"},
          .topics = chunked_vector<offset_commit_request_topic>::single(
            offset_commit_request_topic{
              .name = model::topic{"foo"}, .partitions = {}})};

        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
}

FIXTURE_TEST(offset_commit_and_fetch_request, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);

    static const model::topic topic_foo{"foo"};
    static const model::topic topic_bar{"bar"};
    static const model::topic topic_no_auth{"no_auth"};

    static const kafka::group_id group_foo{"foo-partitions"};
    static const kafka::group_id group_bar{"bar-partitions"};
    static const kafka::group_id group_no_auth{"no_auth-partitions"};

    // v2 supports an error_code
    static constexpr api_version version_with_error{2};
    // v8 supports multiple groups
    static constexpr api_version version_with_groups{8};

    const std::map<
      kafka::group_id,
      std::map<model::topic, std::map<model::partition_id, model::offset>>>
      committed_offsets{
        {group_foo,
         {
           {topic_foo, {{model::partition_id{2}, model::offset{42}}}},
           {topic_no_auth, {{model::partition_id{1}, model::offset{24}}}},
         }},
        {group_bar,
         {
           {topic_bar, {{model::partition_id{1}, model::offset{24}}}},
           {topic_no_auth, {{model::partition_id{0}, model::offset{36}}}},
         }},
        {group_no_auth,
         {
           {topic_no_auth, {{model::partition_id{0}, model::offset{12}}}},
         }},
      };

    for (const auto& topic : {topic_foo, topic_bar, topic_no_auth}) {
        add_topic({model::kafka_namespace, topic}, 3).get();
    }

    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    // Commit offsets
    for (const auto& [group_id, topics] : committed_offsets) {
        offset_commit_request req{.data{.group_id = group_id}};
        req.data.group_instance_id = gr;
        for (const auto& [topic, partitions] : topics) {
            auto& c_topic = req.data.topics.emplace_back(
              offset_commit_request_topic{.name = topic});
            for (const auto& [partition, offset] : partitions) {
                c_topic.partitions.push_back(
                  {.partition_index = partition, .committed_offset = offset});
            }
        }
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }

    const auto fill_request =
      [&committed_offsets](
        auto& req, const kafka::group_id& group_id, bool all_topics) {
          req.group_id = group_id;
          if (!all_topics) {
              req.topics.emplace();
              for (const auto& [topic, partitions] :
                   committed_offsets.at(group_id)) {
                  req.topics->push_back(
                    {.name{topic},
                     .partition_indexes{
                       std::from_range, partitions | std::views::keys}});
              }
          }
      };

    const auto check_response_topics = [&committed_offsets](
                                         kafka::api_version api_version,
                                         auto& resp,
                                         const kafka::group_id& group_id,
                                         bool all_topics) {
        if (group_id == group_no_auth) {
            BOOST_REQUIRE(resp.topics.empty());
            if (api_version >= version_with_error) {
                BOOST_REQUIRE_EQUAL(
                  resp.error_code, error_code::group_authorization_failed);
            }
            return;
        }
        const auto& c_offsets = committed_offsets.at(group_id);
        BOOST_REQUIRE_EQUAL(
          resp.topics.size(), all_topics ? 1 : c_offsets.size());
        for (const auto& topic : resp.topics) {
            const auto t_it = c_offsets.find(topic.name);
            BOOST_REQUIRE(t_it != c_offsets.end());
            BOOST_REQUIRE_EQUAL(topic.partitions.size(), t_it->second.size());
            for (const auto& p : topic.partitions) {
                if (topic.name == topic_no_auth) {
                    BOOST_REQUIRE(resp.errored());
                    BOOST_REQUIRE_EQUAL(p.committed_offset, model::offset{-1});
                    BOOST_REQUIRE_EQUAL(
                      p.error_code, error_code::topic_authorization_failed);
                } else {
                    const auto p_it = t_it->second.find(p.partition_index);
                    BOOST_REQUIRE(p_it != t_it->second.end());
                    BOOST_REQUIRE_EQUAL(p_it->second, p.committed_offset);
                }
            }
        }
    };

    ss::sstring user{"user_name_256"};
    ss::sstring password{"password_256"};
    create_user(user, password);
    authorize(
      user,
      security::acl_operation::describe,
      std::vector<model::topic>{topic_foo, topic_bar});
    authorize(
      user,
      security::acl_operation::describe,
      std::vector<kafka::group_id>{group_bar, group_foo});

    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    auto auth_client = make_kafka_client().get();
    auto auth_deferred = ss::defer([&auth_client] {
        auth_client.stop()
          .then([&auth_client] { auth_client.shutdown(); })
          .get();
    });
    auth_client.connect().get();

    authn_kafka_client<security::scram_sha256_authenticator>(
      auth_client, user, password);

    for (auto api_version = kafka::offset_fetch_handler::min_supported;
         api_version <= kafka::offset_fetch_handler::max_supported;
         ++api_version) {
        for (bool all_topics : {true, false}) {
            if (api_version < version_with_groups) {
                for (const auto& g : committed_offsets | std::views::keys) {
                    offset_fetch_request req;
                    fill_request(req.data, g, all_topics);
                    auto resp
                      = auth_client.dispatch(std::move(req), api_version).get();
                    if (api_version >= version_with_error) {
                        BOOST_REQUIRE_EQUAL(
                          resp.data.errored(),
                          !all_topics || g == group_no_auth);
                    }
                    BOOST_REQUIRE_EQUAL(resp.data.groups.size(), 0);
                    check_response_topics(
                      api_version, resp.data, g, all_topics);
                }
            } else {
                offset_fetch_request req;
                for (const auto& g : committed_offsets | std::views::keys) {
                    fill_request(req.data.groups.emplace_back(), g, all_topics);
                }
                auto resp
                  = auth_client.dispatch(std::move(req), api_version).get();
                BOOST_REQUIRE(resp.data.errored());
                BOOST_REQUIRE_EQUAL(
                  resp.data.groups.size(), committed_offsets.size());
                for (const auto& g : resp.data.groups) {
                    check_response_topics(
                      api_version, g, g.group_id, all_topics);
                }
            }
        }
    }
}

FIXTURE_TEST(block_test, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);

    model::topic t{"topic"};
    kafka::group_id g{"group"};
    kafka::group_instance_id gi("group-instance");
    kafka::group_id ug1{"unrelated_group1"};
    kafka::group_id ug2{"unrelated_group2"};

    add_topic(model::topic_namespace_view{model::kafka_namespace, t}).get();
    wait_for_consumer_offsets_topic(gi);

    auto client = std::make_unique<kafka::client::transport>(
      make_kafka_client().get());
    auto client_stop = [&client] {
        client->stop().then([&client] { client->shutdown(); }).get();
    };
    auto deferred = ss::defer(decltype(client_stop)(client_stop));
    client->connect().get();

    auto g_partition = app.coordinator_ntp_mapper.local().partition_for(g);
    BOOST_REQUIRE(g_partition);
    model::ntp gntp(
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      *g_partition);

    auto can_commit_offset = [&] {
        for (int _ : std::views::iota(0, 5)) {
            auto req = offset_commit_request{.data{
              .group_id = g,
              .group_instance_id = gi,
              .topics = chunked_vector<offset_commit_request_topic>::single(
                offset_commit_request_topic{
                  .name = t,
                  .partitions
                  = chunked_vector<offset_commit_request_partition>::single(
                    offset_commit_request_partition{
                      .partition_index = model::partition_id{0},
                      .committed_offset = model::offset{0}})})}};
            auto res
              = client->dispatch(std::move(req), kafka::api_version(7)).get();
            if (!res.data.errored()) {
                return true;
            }
            if (
              res.data.topics[0].partitions[0].error_code
              != kafka::error_code::not_coordinator) {
                return false;
            }
        }
        return false;
    };

    auto set_blocked = [&](bool blocked) {
        auto res = app._group_manager.local()
                     .set_blocked_for_groups(
                       gntp,
                       // triggering a bug where the wrong group gets unblocked
                       blocked ? chunked_vector<kafka::group_id>{ug1, g, ug2}
                               : chunked_vector<kafka::group_id>{g},
                       blocked)
                     .get();
        BOOST_REQUIRE(res.has_value());
    };

    auto restart = [&] {
        client_stop();
        consumer_offsets_fixture::restart(should_wipe::no);
        wait_for_consumer_offsets_topic(gi);
        wait_for_leader(gntp).get();
        client = std::make_unique<kafka::client::transport>(
          make_kafka_client().get());
        client->connect().get();
    };

    BOOST_REQUIRE(can_commit_offset());

    // block
    set_blocked(true);
    BOOST_REQUIRE(!can_commit_offset());

    // make sure retart keeps it blocked
    restart();
    BOOST_REQUIRE(!can_commit_offset());

    // unblock
    set_blocked(false);
    BOOST_REQUIRE(can_commit_offset());

    // make sure retart keeps it unblocked
    restart();
    BOOST_REQUIRE(can_commit_offset());
}

FIXTURE_TEST(conditional_retention_test, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    // setting to true to begin with, so log_eviction_stm is attached to
    // the partition.
    cfg.get("unsafe_enable_consumer_offsets_delete_retention").set_value(true);
    add_topic(
      model::topic_namespace_view{model::kafka_namespace, model::topic{"foo"}})
      .get();
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    // load some data into the topic via offset_commit requests.
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();
    auto offset = 0;
    auto rand_offset_commit = [&] {
        auto req_part = offset_commit_request_partition{
          .partition_index = model::partition_id{0},
          .committed_offset = model::offset{offset++}};
        auto topic = offset_commit_request_topic{
          .name = model::topic{"foo"}, .partitions = {std::move(req_part)}};

        return offset_commit_request{.data{
          .group_id = kafka::group_id{fmt::format("foo-{}", offset)},
          .topics = chunked_vector<offset_commit_request_topic>::single(
            std::move(topic))}};
    };
    for (int i = 0; i < 10; i++) {
        auto req = rand_offset_commit();
        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
    auto part = app.partition_manager.local().get(
      model::ntp{
        model::kafka_namespace,
        model::kafka_consumer_offsets_topic,
        model::partition_id{0}});
    BOOST_REQUIRE(part);
    auto log = part->log();
    storage::ntp_config::default_overrides ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion
                                 | model::cleanup_policy_bitflags::compaction;
    log->set_overrides(ov);
    log->notify_compaction_update();
    log->flush().get();
    log->force_roll().get();
    for (auto retention_enabled : {false, true}) {
        // number of partitions of CO topic.
        cfg.get("unsafe_enable_consumer_offsets_delete_retention")
          .set_value(retention_enabled);
        // attempt a GC on the partition log.
        // evict the first segment.
        storage::gc_config gc_cfg{model::timestamp::max(), 1};
        log->gc(gc_cfg).get();
        // Check if retention works
        try {
            tests::cooperative_spin_wait_with_timeout(5s, [&] {
                return log.get()->offsets().start_offset > model::offset{0};
            }).get();
        } catch (const ss::timed_out_error& e) {
            if (retention_enabled) {
                std::rethrow_exception(std::make_exception_ptr(e));
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(consumer_group_decode) {
    {
        // snatched from a log message after a franz-go client joined
        auto data = bytes_to_iobuf(
          base64_to_bytes("AAEAAAADAAJ0MAACdDEAAnQyAAAACAAAAAAAAAAAAAAAAA=="));
        const auto topics = group::decode_consumer_subscriptions(
          std::move(data));
        BOOST_REQUIRE(
          topics
          == absl::node_hash_set<model::topic>(
            {model::topic("t0"), model::topic("t1"), model::topic("t2")}));
    }
}
