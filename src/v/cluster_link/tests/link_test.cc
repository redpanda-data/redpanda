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

#include "absl/container/flat_hash_map.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "cluster_link/link.h"
#include "cluster_link/manager.h"
#include "cluster_link/replication/tests/deps_test_impl.h"
#include "cluster_link/tests/deps.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

using kafka::data::rpc::test::fake_topic_creator;

using default_config_provider
  = cluster_link::replication::tests::test_config_provider;
using data_src_factory
  = cluster_link::replication::tests::random_data_source_factory;
using data_sink_factory
  = cluster_link::replication::tests::accounting_sink_factory;

namespace cluster_link::tests {

using ::cluster::cluster_link::table;

class link_test;
namespace {

class test_link : public link {
public:
    test_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      ss::lowres_clock::duration task_reconciler_interval,
      link_test* link_test,
      model::metadata_ptr metadata,
      std::unique_ptr<kafka::client::cluster> cluster_connection);

    ss::future<> start() override;
    ss::future<> stop() noexcept override;

private:
    link_test* _link_test;
};

class link_test_factory : public link_factory {
public:
    link_test_factory(
      link_test* link_test, ss::lowres_clock::duration task_reconciler_interval)
      : _link_test(link_test)
      , _task_reconciler_interval(task_reconciler_interval) {}

    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata_ptr metadata,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        return std::make_unique<test_link>(
          self,
          link_id,
          manager,
          _task_reconciler_interval,
          _link_test,
          std::move(metadata),
          std::move(cluster_connection));
    }

private:
    link_test* _link_test;
    ss::lowres_clock::duration _task_reconciler_interval;
};
} // namespace

class link_test_base : public seastar_test {
public:
    virtual ss::future<> SetUpAsync() override {
        setup_cluster_mock();
        _partition_leader_cache_impl
          = std::make_unique<fake_partition_leader_cache_impl>();
        _partition_manager_proxy
          = std::make_unique<fake_partition_manager_proxy>();
        co_await _table.start();
    }

    virtual ss::future<> TearDownAsync() override {
        co_await _table.stop();
        _partition_manager_proxy.reset();
        _partition_leader_cache_impl.reset();
    }

    ss::future<> upsert_link(model::id_t id, model::metadata metadata) {
        co_await _table.local().apply_update(
          ::cluster::cluster_link::testing::create_upsert_command(
            ::model::offset{id()}, std::move(metadata)));
        _manager->on_link_change(id, {});
    }

    ss::future<> remove_link(const model::name_t& name) {
        auto id = _table.local().find_id_by_name(name);
        co_await _table.local().apply_update(
          ::cluster::cluster_link::testing::create_remove_command(name, false));
        if (id.has_value()) {
            _manager->on_link_change(id.value(), {});
        }
    }

    void run_callbacks(uuid_t id) {
        for (const auto& [_, cb] : _callbacks) {
            cb(id);
        }
    }

    using notification_id = named_type<size_t, struct test_notification_tag>;
    using notification_callback = ss::noncopyable_function<void(uuid_t)>;

    notification_id
    register_callback(ss::noncopyable_function<void(uuid_t)> cb) {
        auto it = _callbacks.insert({++_latest_id, std::move(cb)});
        vassert(it.second, "Invalid duplicate in callbacks");
        return _latest_id;
    }

    void unregister_callback(notification_id id) { _callbacks.erase(id); }

protected:
    kafka::client::cluster_mock _cluster_mock;
    std::unique_ptr<fake_partition_leader_cache_impl>
      _partition_leader_cache_impl;
    std::unique_ptr<fake_partition_manager_proxy> _partition_manager_proxy;
    ss::sharded<table> _table;

    std::unique_ptr<manager> _manager;
    config::mock_property<int16_t> _default_topic_replication{3};

    absl::flat_hash_map<notification_id, ss::noncopyable_function<void(uuid_t)>>
      _callbacks;
    notification_id _latest_id{0};

private:
    void setup_cluster_mock() {
        _cluster_mock.register_default_handlers();
        _cluster_mock.add_broker(
          ::model::node_id(0), net::unresolved_address{"localhost", 9092});
        _cluster_mock.add_broker(
          ::model::node_id(1), net::unresolved_address{"localhost", 9093});
        _cluster_mock.add_broker(
          ::model::node_id(2), net::unresolved_address{"localhost", 9094});
    }
};

class link_test : public link_test_base {
public:
    static constexpr auto task_reconciler_interval = 1s;
    virtual ss::future<> SetUpAsync() override {
        co_await link_test_base::SetUpAsync();
        auto tmc = std::make_unique<fake_topic_metadata_cache>();
        _tmc = tmc.get();
        _manager = std::make_unique<manager>(
          ::model::node_id(0),
          std::make_unique<fake_partition_leader_cache>(
            _partition_leader_cache_impl.get()),
          std::make_unique<fake_partition_manager>(
            _partition_manager_proxy.get()),
          std::move(tmc),
          std::make_unique<fake_topic_creator>(
            [](const cluster::topic_configuration&) {},
            [](const cluster::topic_properties_update&) {},
            [](const ::model::ntp&, ::model::node_id) {},
            [](::model::topic_namespace_view, int32_t, ::model::node_id) {
                return cluster::errc::success;
            },
            _default_topic_replication.bind()),
          std::make_unique<fake_security_service>(),
          std::make_unique<test_link_registry>(&_table.local()),
          std::make_unique<link_test_factory>(this, 1s),
          std::make_unique<cluster_mock_factory>(&_cluster_mock),
          std::make_unique<test_consumer_group_router>(),
          std::make_unique<test_partition_metadata_provider>(),
          std::make_unique<test_kafka_rpc_client_service>(_tmc),
          std::make_unique<fake_members_table_provider>(),
          task_reconciler_interval,
          _default_topic_replication.bind(),
          ss::default_scheduling_group());
    }

    virtual ss::future<> TearDownAsync() override {
        _tmc = nullptr;
        _manager.reset(nullptr);
        co_await link_test_base::TearDownAsync();
    }

    void add_link_to_list(uuid_t id, test_link* link) {
        _links.emplace(id, link);
        run_callbacks(id);
    }

    void remove_link_from_list(uuid_t id) {
        _links.erase(id);
        run_callbacks(id);
    }

protected:
    absl::flat_hash_map<uuid_t, test_link*> _links;
    fake_topic_metadata_cache* _tmc{nullptr};
};

class link_test_manager_started : public link_test {
public:
    ss::future<> SetUpAsync() override {
        co_await link_test::SetUpAsync();
        co_await _manager->start();
    }

    ss::future<> TearDownAsync() override {
        co_await _manager->stop();
        co_await link_test::TearDownAsync();
    }
};

namespace {
test_link::test_link(
  ::model::node_id self,
  model::id_t link_id,
  manager* manager,
  ss::lowres_clock::duration task_reconciler_interval,
  link_test* link_test,
  model::metadata_ptr metadata,
  std::unique_ptr<kafka::client::cluster> cluster_connection)
  : link(
      self,
      link_id,
      manager,
      task_reconciler_interval,
      std::move(metadata),
      std::move(cluster_connection),
      std::make_unique<default_config_provider>(),
      std::make_unique<data_src_factory>(),
      std::make_unique<data_sink_factory>())
  , _link_test(link_test) {}

ss::future<> test_link::start() {
    co_await link::start();
    _link_test->add_link_to_list(get_config()->uuid, this);
}

ss::future<> test_link::stop() noexcept {
    _link_test->remove_link_from_list(get_config()->uuid);
    co_await link::stop();
}
} // namespace

TEST_F_CORO(link_test, start_with_table_entries) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, co_await link.copy());
    co_await _manager->start();
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    EXPECT_EQ(*(it->second->get_config()), link);
    co_await _manager->stop();
}

TEST_F_CORO(link_test_manager_started, test_create_link_and_update) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, co_await link.copy());
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    EXPECT_EQ(*(it->second->get_config()), link);

    model::metadata updated_link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{
        .bootstrap_servers{net::unresolved_address{"localhost", 9092}}}};
    co_await upsert_link(link_id, co_await updated_link.copy());

    it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    for (auto i = 0; i < 5; ++i) {
        if (*(it->second->get_config()) == updated_link) {
            break;
        }
        co_await ss::sleep(100ms);
    }
    ASSERT_EQ_CORO(*(it->second->get_config()), updated_link)
      << "Link configuration did not update after 5 attempts";
}

TEST_F_CORO(link_test_manager_started, test_remove_link) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, std::move(link));
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;

    co_await remove_link(model::name_t("link1"));
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link deletion";
    it = _links.find(link_uuid);
    EXPECT_EQ(it, _links.end())
      << "Link with UUID: " << link_uuid << " was not removed";
}

TEST_F_CORO(link_test_manager_started, test_remove_non_existant_link) {
    _manager->on_link_change(model::id_t(1), {});
    return ss::now();
}

class evil_link : public link {
public:
    using link::link;

    ss::future<> start() override {
        co_await link::start();
        static bool start_errored = false;
        if (start_errored) {
            start_errored = false;
            _running = true;
            co_return;
        }
        start_errored = true;
        throw std::runtime_error("Evil link start method failed");
    }

    ss::future<> stop() noexcept override { co_await link::stop(); }

    bool running() const { return _running; }

private:
    bool _running{false};
};

class evil_link_factory : public link_factory {
public:
    std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata_ptr metadata,
      std::unique_ptr<kafka::client::cluster> cluster_connection) override {
        return std::make_unique<evil_link>(
          self,
          link_id,
          manager,
          1s,
          std::move(metadata),
          std::move(cluster_connection),
          std::make_unique<default_config_provider>(),
          std::make_unique<data_src_factory>(),
          std::make_unique<data_sink_factory>());
    }
};

class evil_link_test : public link_test_base {
public:
    static constexpr auto task_reconciler_interval = 1s;
    ss::future<> SetUpAsync() override {
        co_await link_test_base::SetUpAsync();
        auto elf = std::make_unique<evil_link_factory>();
        _elf = elf.get();
        auto tmc = std::make_unique<fake_topic_metadata_cache>();
        _tmc = tmc.get();
        _manager = std::make_unique<manager>(
          ::model::node_id(0),
          std::make_unique<fake_partition_leader_cache>(
            _partition_leader_cache_impl.get()),
          std::make_unique<fake_partition_manager>(
            _partition_manager_proxy.get()),
          std::move(tmc),
          std::make_unique<fake_topic_creator>(
            [](const cluster::topic_configuration&) {},
            [](const cluster::topic_properties_update&) {},
            [](const ::model::ntp&, ::model::node_id) {},
            [](::model::topic_namespace_view, int32_t, ::model::node_id) {
                return cluster::errc::success;
            },
            _default_topic_replication.bind()),
          std::make_unique<fake_security_service>(),
          std::make_unique<test_link_registry>(&_table.local()),
          std::move(elf),
          std::make_unique<cluster_mock_factory>(&_cluster_mock),
          std::make_unique<test_consumer_group_router>(),
          std::make_unique<test_partition_metadata_provider>(),
          std::make_unique<test_kafka_rpc_client_service>(_tmc),
          std::make_unique<fake_members_table_provider>(),
          task_reconciler_interval,
          _default_topic_replication.bind(),
          ss::default_scheduling_group());
        co_await _manager->start();
    }

    ss::future<> TearDownAsync() override {
        co_await _manager->stop();
        _tmc = nullptr;
        _elf = nullptr;
        _manager.reset(nullptr);

        co_await link_test_base::TearDownAsync();
    }

protected:
    evil_link_factory* _elf;
    fake_topic_metadata_cache* _tmc{nullptr};
};

TEST_F_CORO(evil_link_test, test_evil_link_start_stop) {
    auto name = model::name_t("link1");
    model::metadata link{
      .name = name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};
    model::id_t link_id(1);

    co_await upsert_link(link_id, std::move(link));

    // Enough time for the upsert callback to fire but no link should be present
    co_await ss::sleep(500ms);

    auto report = _manager->get_task_status_report();

    EXPECT_TRUE(report.link_reports.empty())
      << "Link should not be present yet";

    // Link reconciler loop takes 10 seconds to run
    co_await ss::sleep(11s);

    report = _manager->get_task_status_report();
    auto link_report = report.link_reports.find(name);
    EXPECT_NE(link_report, report.link_reports.end())
      << "Link should be present after reconciler loop";

    co_await remove_link(name);
    // Link reconciler loop takes 10 seconds to run
    co_await ss::sleep(11s);
    report = _manager->get_task_status_report();
    EXPECT_TRUE(report.link_reports.empty())
      << "Link should be removed after reconciler loop";
}

} // namespace cluster_link::tests
