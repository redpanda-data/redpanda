/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/tests/deps.h"
#include "cluster_link/topic_reconciler.h"
#include "config/mock_property.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

using namespace std::chrono_literals;

using kafka::data::rpc::test::fake_topic_creator;

namespace cluster_link::tests {
namespace {
static const auto default_link_name = model::name_t("test_link");
}
class topic_reconciler_test : public seastar_test {
public:
    virtual ss::future<> SetUpAsync() override {
        co_await _table.start();
        _link_registry = std::make_unique<test_link_registry>(&_table.local());
        _ftmc = std::make_unique<fake_topic_metadata_cache>();
        _ftc = std::make_unique<fake_topic_creator>(
          [this](const cluster::topic_configuration& tp_cfg) {
              _ftmc->set_topic_config(tp_cfg);
          },
          [this](const cluster::topic_properties_update& update) {
              _ftmc->update_topic_config(update);
          },
          [](const ::model::ntp&, ::model::node_id) {
              // ignored - not taking an action when an ntp is created (e.g.
              // elect a leader)
          },
          [this](
            ::model::topic_namespace_view tp_ns,
            int32_t partition_count,
            ::model::node_id) {
              _ftmc->set_partition_count(tp_ns, partition_count);
              return cluster::errc::success;
          },
          _default_topic_replication.bind());

        _reconciler = std::make_unique<topic_reconciler>(
          _ftc.get(),
          _ftmc.get(),
          _link_registry.get(),
          1s,
          _default_topic_replication.bind(),
          ss::default_scheduling_group());
        co_await _reconciler->start();

        set_required_topic_properties(
          {{"max.message.bytes", "1048576"},
           {"cleanup.policy", "delete"},
           {"message.timestamp.type", "CreateTime"}});

        co_await upsert_link(
          model::metadata{
            .name = default_link_name,
            .uuid = model::uuid_t(::uuid_t::create())});

        auto ids = _link_registry->get_all_link_ids();
        ASSERT_EQ_CORO(ids.size(), 1);
        _created_link_id = ids[0];
    }

    virtual ss::future<> TearDownAsync() override {
        co_await _reconciler->stop();
        _reconciler.reset(nullptr);

        _ftc.reset(nullptr);
        _ftmc.reset(nullptr);
        _link_registry.reset(nullptr);
        co_await _table.stop();
    }

    ss::future<> upsert_link(model::metadata md) {
        auto id = model::id_t(_next_link_id++);
        return ss::do_with(
          id, std::move(md), [this](model::id_t& id, model::metadata& md) {
              return _table.invoke_on_all([id, &md](
                                            cluster::cluster_link::table& t) {
                  return md.copy().then([id, &t](model::metadata md) {
                      return t
                        .apply_update(
                          cluster::cluster_link::testing::create_upsert_command(
                            ::model::offset{id()}, std::move(md)))
                        .then([](std::error_code ec) {
                            vassert(
                              ec.value() == 0,
                              "failed to upsert link: {}",
                              ec.message());
                        });
                  });
              });
          });
    }

    model::id_t get_link_id() const noexcept { return _created_link_id; }

    ss::future<>
    add_mirror_topic(model::id_t id, model::add_mirror_topic_cmd cmd) {
        auto batch
          = cluster::cluster_link::testing::create_add_mirror_topic_command(
            id, cmd.copy());
        auto ec = co_await _table.local().apply_update(std::move(batch));
        vassert(
          ec.value() == 0, "failed to add mirror topic: {}", ec.message());
    }

    ss::future<> add_mirror_topic(
      model::id_t id,
      ::model::topic topic,
      int32_t partition_count,
      std::optional<int16_t> replication_factor,
      chunked_hash_map<ss::sstring, ss::sstring> topic_configs = {}) {
        topic_configs.insert(
          _required_topic_properties.begin(), _required_topic_properties.end());
        model::mirror_topic_metadata tpmd{
          .source_topic_name = topic,
          .destination_topic_id = ::model::topic_id{::uuid_t::create()},
          .partition_count = partition_count,
          .replication_factor = replication_factor,
          .topic_configs = std::move(topic_configs),
        };

        return add_mirror_topic(
          id,
          model::add_mirror_topic_cmd{
            .topic = topic, .metadata = std::move(tpmd)});
    }

    ss::future<> update_mirror_topic_properties(
      model::id_t id, model::update_mirror_topic_properties_cmd cmd) {
        auto batch = cluster::cluster_link::testing::
          create_update_mirror_topic_properties_command(id, cmd.copy());
        auto ec = co_await _table.local().apply_update(std::move(batch));
        vassert(
          ec.value() == 0,
          "failed to update mirror topic properties: {}",
          ec.message());
    }

    void set_required_topic_properties(
      chunked_hash_map<ss::sstring, ss::sstring> props) {
        _required_topic_properties = std::move(props);
    }

    const chunked_hash_map<ss::sstring, ss::sstring>&
    get_required_topic_properties() const {
        return _required_topic_properties;
    }

    fake_topic_metadata_cache* metadata_cache() const { return _ftmc.get(); }

    topic_reconciler* reconciler() const { return _reconciler.get(); }

    link_registry* link_registry() const { return _link_registry.get(); }

    config::mock_property<int16_t>& default_topic_replication() {
        return _default_topic_replication;
    }

private:
    ss::sharded<cluster::cluster_link::table> _table;
    std::unique_ptr<test_link_registry> _link_registry;
    std::unique_ptr<fake_topic_metadata_cache> _ftmc;
    std::unique_ptr<fake_topic_creator> _ftc;
    std::unique_ptr<topic_reconciler> _reconciler;

    chunked_hash_map<ss::sstring, ss::sstring> _required_topic_properties;
    model::id_t _created_link_id;

    model::id_t _next_link_id{1};

    config::mock_property<int16_t> _default_topic_replication{3};
};

TEST_F_CORO(topic_reconciler_test, test_topic_creation_and_property_updates) {
    ::model::topic_namespace topic{
      ::model::kafka_namespace, ::model::topic{"test_topic"}};

    co_await add_mirror_topic(get_link_id(), topic.tp, 1, 1);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic}] {
          return metadata_cache()->find_topic_cfg(topic).has_value();
      });

    const auto topic_cfg = metadata_cache()->find_topic_cfg(topic).value();
    EXPECT_EQ(topic_cfg.tp_ns, topic);
    EXPECT_EQ(topic_cfg.partition_count, 1);
    EXPECT_EQ(topic_cfg.replication_factor, 1);

    EXPECT_EQ(topic_cfg.properties.batch_max_bytes, 1048576);
    EXPECT_EQ(
      topic_cfg.properties.cleanup_policy_bitflags,
      ::model::cleanup_policy_bitflags::deletion);
    EXPECT_EQ(
      topic_cfg.properties.timestamp_type,
      ::model::timestamp_type::create_time);

    co_await update_mirror_topic_properties(
      get_link_id(),
      model::update_mirror_topic_properties_cmd{
        .topic = topic.tp,
        .partition_count = 3,
        .replication_factor = 3,
        .topic_configs = {
          {"max.message.bytes", "2097152"},
          {"cleanup.policy", "compact"},
          {"message.timestamp.type", "LogAppendTime"}}});
    reconciler()->trigger(get_link_id());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic}] {
          const auto topic_cfg = metadata_cache()->find_topic_cfg(topic);
          if (!topic_cfg.has_value()) {
              return false;
          }
          return topic_cfg->partition_count == 3
                 && topic_cfg->replication_factor == 3
                 && topic_cfg->properties.batch_max_bytes == 2097152
                 && topic_cfg->properties.cleanup_policy_bitflags
                      == ::model::cleanup_policy_bitflags::compaction
                 && topic_cfg->properties.timestamp_type
                      == ::model::timestamp_type::append_time;
      });
}

TEST_F_CORO(topic_reconciler_test, test_topic_failure) {
    ::model::topic_namespace topic{
      ::model::kafka_namespace, ::model::topic{"test_topic"}};

    co_await add_mirror_topic(get_link_id(), topic.tp, 3, 1);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic}] {
          return metadata_cache()->find_topic_cfg(topic).has_value();
      });

    // We should see the mirror topic enter the failed state as the partition
    // count appears to go backwards, indicating that the topic may have been
    // deleted and re-created
    co_await update_mirror_topic_properties(
      get_link_id(),
      model::update_mirror_topic_properties_cmd{
        .topic = topic.tp,
        .partition_count = 1,
        .replication_factor = 1,
        .topic_configs = {
          {"max.message.bytes", "2097152"},
          {"cleanup.policy", "compact"},
          {"message.timestamp.type", "LogAppendTime"}}});
    reconciler()->trigger(get_link_id());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic}] {
          auto link = link_registry()->find_link_by_name(default_link_name);
          const auto& mirror_topics = link->state.mirror_topics;
          return mirror_topics.contains(topic.tp)
                 && mirror_topics.at(topic.tp).status
                      == model::mirror_topic_status::failed;
      });

    // Now update mirror topic properties again, but there should be no change
    co_await update_mirror_topic_properties(
      get_link_id(),
      model::update_mirror_topic_properties_cmd{
        .topic = topic.tp,
        .partition_count = 3,
        .replication_factor = 3,
        .topic_configs = {
          {"max.message.bytes", "2097152"},
          {"cleanup.policy", "compact"},
          {"message.timestamp.type", "LogAppendTime"}}});
    reconciler()->trigger(get_link_id());

    co_await ss::sleep(2s);
    const auto topic_cfg = metadata_cache()->find_topic_cfg(topic);
    ASSERT_TRUE_CORO(topic_cfg.has_value());
    EXPECT_EQ(topic_cfg->replication_factor, 1);
}

TEST_F_CORO(topic_reconciler_test, test_no_rf_set) {
    ::model::topic_namespace topic{
      ::model::kafka_namespace, ::model::topic{"test_topic"}};
    ::model::topic_namespace topic2{
      ::model::kafka_namespace, ::model::topic{"test_topic2"}};

    default_topic_replication().update(1);

    co_await add_mirror_topic(get_link_id(), topic.tp, 1, std::nullopt);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic}] {
          return metadata_cache()->find_topic_cfg(topic).has_value();
      });

    const auto topic_cfg = metadata_cache()->find_topic_cfg(topic).value();
    EXPECT_EQ(topic_cfg.tp_ns, topic);
    EXPECT_EQ(topic_cfg.partition_count, 1);
    EXPECT_EQ(topic_cfg.replication_factor, 1);

    default_topic_replication().update(3);

    co_await add_mirror_topic(get_link_id(), topic2.tp, 1, std::nullopt);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, topic = ::model::topic_namespace_view{topic2}] {
          return metadata_cache()->find_topic_cfg(topic).has_value();
      });

    const auto topic_cfg2 = metadata_cache()->find_topic_cfg(topic2).value();
    EXPECT_EQ(topic_cfg2.tp_ns, topic2);
    EXPECT_EQ(topic_cfg2.partition_count, 1);
    EXPECT_EQ(topic_cfg2.replication_factor, 3);
}
} // namespace cluster_link::tests
