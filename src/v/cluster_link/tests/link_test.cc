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
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/offset_fetch.h"
#include "schema/tests/fake_registry.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
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
    schema::fake_registry _fake_schema_registry;
    config::mock_property<int16_t> _default_topic_replication{3};

    absl::flat_hash_map<notification_id, ss::noncopyable_function<void(uuid_t)>>
      _callbacks;
    notification_id _latest_id{0};

private:
    template<typename Api>
    void advertise_api() {
        _cluster_mock.default_supported_versions[Api::key] = {
          .min = Api::min_valid, .max = Api::max_valid};
    }

    void setup_cluster_mock() {
        _cluster_mock.register_default_handlers();
        _cluster_mock.add_broker(
          ::model::node_id(0), net::unresolved_address{"localhost", 9092});
        _cluster_mock.add_broker(
          ::model::node_id(1), net::unresolved_address{"localhost", 9093});
        _cluster_mock.add_broker(
          ::model::node_id(2), net::unresolved_address{"localhost", 9094});
        // Advertise (via the mock's default fallback) the APIs the link broker
        // preflight requires that are not in the mock's built-in defaults, so
        // the kafka-path check passes and the Schema Registry checks are
        // reached.
        advertise_api<kafka::find_coordinator_api>();
        advertise_api<kafka::list_groups_api>();
        advertise_api<kafka::list_offsets_api>();
        advertise_api<kafka::offset_fetch_api>();
    }
};

class link_test : public link_test_base {
public:
    static constexpr auto task_reconciler_interval = 1s;
    virtual ss::future<> SetUpAsync() override {
        co_await link_test_base::SetUpAsync();
        auto tmc = std::make_unique<fake_topic_metadata_cache>();
        _tmc = tmc.get();
        auto sr_prober = std::make_unique<fake_source_sr_prober>();
        _fake_sr_prober = sr_prober.get();
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
          sr_preflight_checker::make_default(
            _fake_schema_registry, std::move(sr_prober)),
          nullptr,
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
    fake_source_sr_prober* _fake_sr_prober{nullptr};
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
          sr_preflight_checker::make_default(
            _fake_schema_registry, std::make_unique<fake_source_sr_prober>()),
          nullptr,
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

namespace {

/// A task that refreshes metadata from the source cluster, mirroring the
/// first step of source_topic_syncer::run_impl. request_metadata_update()
/// takes no abort source: its waits (metadata update lock, seed reconnect
/// backoff) answer only to the kafka::client::cluster's internal abort
/// source.
class metadata_refresh_task : public task {
public:
    static constexpr auto task_name = "metadata_refresh_task";
    explicit metadata_refresh_task(link* link)
      : task(link, 100ms, task_name) {}

    bool should_start_impl(ss::shard_id, ::model::node_id) const override {
        return true;
    }
    bool should_stop_impl(ss::shard_id, ::model::node_id) const override {
        return false;
    }
    void update_config(const model::metadata&) override {}
    model::enabled_t is_enabled() const final { return model::enabled_t::yes; }

    ss::future<state_transition> run_impl(ss::abort_source&) override {
        try {
            co_await get_link()
              ->get_cluster_connection()
              .request_metadata_update();
        } catch (const std::exception& e) {
            co_return state_transition{
              .desired_state = model::task_state::link_unavailable,
              .reason = ssx::sformat(
                "Failed to update metadata: {}", e.what())};
        }
        co_return state_transition{
          .desired_state = model::task_state::active, .reason = "ok"};
    }
};

class metadata_refresh_task_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept override {
        return metadata_refresh_task::task_name;
    }
    std::unique_ptr<task> create_task(link* link) override {
        return std::make_unique<metadata_refresh_task>(link);
    }
};

} // namespace

// Regression test for deadlock between `link::stop()` and `cluster::stop()`.
TEST_F_CORO(
  link_test_manager_started,
  stop_completes_with_task_wedged_in_source_cluster) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    auto md = ss::make_lw_shared<const model::metadata>(model::metadata{
      .name = model::name_t("wedged_link"),
      .uuid = link_uuid,
      .connection = model::connection_config{}});

    // A source cluster connection with no seed brokers: start() succeeds
    // vacuously (nothing to connect to).
    auto wedged_link = std::make_unique<link>(
      ::model::node_id(0),
      model::id_t(1),
      _manager.get(),
      1s,
      md,
      std::make_unique<kafka::client::cluster>(
        kafka::client::connection_configuration{
          .client_id = "wedge-test",
        }),
      std::make_unique<default_config_provider>(),
      std::make_unique<data_src_factory>(),
      std::make_unique<data_sink_factory>());
    co_await wedged_link->start();

    // Now point it at a seed broker that refuses connections, with a large
    // connection_timeout so the reconnect loop parks in long backoff
    // sleeps — the production wedge for a link whose source cluster is
    // unreachable.
    wedged_link->get_cluster_connection().update_configuration(
      kafka::client::connection_configuration{
        .initial_brokers = {net::unresolved_address{"127.0.0.1", 1}},
        .client_id = "wedge-test",
        .connection_timeout = 10min,
      });

    metadata_refresh_task_factory tf;
    auto res = co_await wedged_link->register_task(&tf);
    ASSERT_TRUE_CORO(res.has_value());

    // Let the task run into the wedge: connect to the dead seed, fail,
    // and park in the reconnect backoff.
    co_await ss::sleep(1s);

    // Stopping the link must complete even with the task fiber wedged in
    // the source cluster's reconnect backoff.
    co_await wedged_link->stop();
}

// --- Schema Registry API-sync preflight checks ---

namespace {
namespace ppsr = pandaproxy::schema_registry;
using sr_cfg = model::schema_registry_sync_config;

// Matches a cl_result<void> that is an error carrying the given errc. Reports
// the actual state (value, or the mismatching code) on failure. Usable inside
// coroutine tests because EXPECT_THAT is non-fatal and never returns.
MATCHER_P(IsLinkError, expected_code, "") {
    if (arg.has_value()) {
        *result_listener << "is a success, expected error";
        return false;
    }
    *result_listener << "error code "
                     << static_cast<int>(arg.assume_error().code()) << " ("
                     << arg.assume_error().message() << ")";
    return arg.assume_error().code() == expected_code;
}

// Builds link metadata whose kafka connection targets the fixture's broker
// mock (so the kafka preflight passes) and whose Schema Registry sync config is
// in API mode with the given destination mapping and source context filter.
model::metadata make_api_sr_metadata(
  ss::sstring name,
  std::optional<sr_cfg::destination_mapping_t> destination,
  chunked_vector<ss::sstring> filter_contexts = {},
  chunked_vector<ss::sstring> filter_subjects = {}) {
    model::metadata md{
      .name = model::name_t(std::move(name)),
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{
        .bootstrap_servers{net::unresolved_address{"localhost", 9092}}}};
    sr_cfg::shadow_schema_registry_api api;
    api.source_url = "http://source.example:8081";
    api.filter.contexts = std::move(filter_contexts);
    api.filter.subjects = std::move(filter_subjects);
    api.destination = std::move(destination);
    md.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
    return md;
}

sr_cfg::destination_mapping_t identity_mapping() {
    return sr_cfg::destination_mapping_t{sr_cfg::identity_context_mapping{}};
}

sr_cfg::destination_mapping_t
exact_mapping(ss::sstring source, ss::sstring destination) {
    sr_cfg::exact_context_mapping mapping;
    mapping.mappings.emplace(std::move(source), std::move(destination));
    return sr_cfg::destination_mapping_t{std::move(mapping)};
}

ss::future<> seed_subject(
  schema::fake_registry& reg,
  ppsr::context ctx,
  ss::sstring sub,
  ppsr::is_deleted deleted = ppsr::is_deleted::no) {
    co_await reg.import_schema(
      ppsr::stored_schema{
        .schema = ppsr::
          subject_schema{ppsr::context_subject{std::move(ctx), ppsr::subject{std::move(sub)}}, ppsr::schema_definition{ppsr::schema_definition::raw_string{R"({"type":"string"})"}, ppsr::schema_type::avro}},
        .version = ppsr::schema_version{1},
        .id = ppsr::schema_id{1},
        .deleted = deleted});
}
} // namespace

// A link that is not in Schema Registry API mode skips both SR checks: the
// source prober is not consulted even though it is primed to fail.
TEST_F_CORO(link_test_manager_started, sr_preflight_skipped_when_not_api_mode) {
    _fake_sr_prober->error = err_info(
      errc::link_sr_unreachable, "prober must not be consulted");
    model::metadata md{
      .name = model::name_t("link1"),
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{
        .bootstrap_servers{net::unresolved_address{"localhost", 9092}}}};
    auto res = co_await _manager->test_connection(std::move(md));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(0));
}

// Source reachable and the destination empty: preflight passes, and the source
// probe actually ran.
TEST_F_CORO(link_test_manager_started, sr_preflight_reachable_and_empty_ok) {
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping()));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(1));
}

// The prober reporting an error surfaces as link_sr_unreachable.
TEST_F_CORO(link_test_manager_started, sr_preflight_source_unreachable) {
    _fake_sr_prober->error = err_info(
      errc::link_sr_unreachable, "connection refused");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping()));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_unreachable));
}

// Identity mapping, select-all filter: a populated destination context fails.
TEST_F_CORO(link_test_manager_started, sr_preflight_identity_target_not_empty) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxA"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping()));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// Identity mapping: a populated in-scope destination context fails even when
// the source has no such context. Only the destination registry is inspected
// for emptiness (the prober reports nothing here), so a context present only in
// the destination is still caught.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_identity_target_absent_from_source) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxA"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping(), {".ctxA"}));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// Exact mapping keys emptiness off the destination context name, not the
// source: a subject in the source-named context does not fail the check.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_exact_mapping_checks_destination) {
    co_await seed_subject(_fake_schema_registry, ppsr::context{".src"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", exact_mapping(".src", ".dst")));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(1));
}

// Exact mapping: a subject in the destination context fails the check.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_exact_mapping_destination_not_empty) {
    co_await seed_subject(_fake_schema_registry, ppsr::context{".dst"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", exact_mapping(".src", ".dst")));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// The context filter narrows the identity target set: a populated context
// excluded by the filter is out of scope.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_identity_filtered_context_ignored) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxB"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping(), {".ctxA"}));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(1));
}

// A subjects-only filter selects the source context parsed from the qualified
// subject, so identity mapping imports into it and its populated target is
// checked. Guards against dropping filter.subjects from target resolution.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_identity_subject_filter_selects) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxB"}, "sub1");
    // Qualified subject ":.ctxB:sub1" selects source context ".ctxB".
    auto res = co_await _manager->test_connection(make_api_sr_metadata(
      "link1", identity_mapping(), /*filter_contexts=*/{}, {":.ctxB:sub1"}));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// A context selected by neither filter.contexts nor filter.subjects is out of
// scope, even when populated.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_identity_subject_filter_excludes) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxB"}, "sub1");
    auto res = co_await _manager->test_connection(make_api_sr_metadata(
      "link1", identity_mapping(), /*filter_contexts=*/{}, {":.ctxA:sub1"}));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(1));
}

// Exact mapping whose source is excluded by the filter is inert, so its
// populated destination context does not fail the check.
TEST_F_CORO(
  link_test_manager_started, sr_preflight_exact_mapping_filtered_source_inert) {
    co_await seed_subject(_fake_schema_registry, ppsr::context{".dst"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", exact_mapping(".src", ".dst"), {".other"}));
    ASSERT_TRUE_CORO(res.has_value());
    EXPECT_THAT(_fake_sr_prober->call_count, testing::Eq(1));
}

// A soft-deleted subject still occupies the context namespace and counts as
// non-empty (include_deleted::yes).
TEST_F_CORO(
  link_test_manager_started, sr_preflight_soft_deleted_counts_as_not_empty) {
    co_await seed_subject(
      _fake_schema_registry,
      ppsr::context{".ctxA"},
      "sub1",
      ppsr::is_deleted::yes);
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping()));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// An unset destination mapping defaults to identity mapping.
TEST_F_CORO(
  link_test_manager_started,
  sr_preflight_unset_destination_defaults_to_identity) {
    co_await seed_subject(
      _fake_schema_registry, ppsr::context{".ctxA"}, "sub1");
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", std::nullopt));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_target_not_empty));
}

// A fault querying the destination registry surfaces as
// link_sr_verification_failed.
TEST_F_CORO(
  link_test_manager_started,
  sr_preflight_destination_fault_verification_failed) {
    _fake_schema_registry.set_inject_failures(
      std::make_exception_ptr(std::runtime_error("registry unavailable")));
    auto res = co_await _manager->test_connection(
      make_api_sr_metadata("link1", identity_mapping()));
    EXPECT_THAT(res, IsLinkError(errc::link_sr_verification_failed));
}

} // namespace cluster_link::tests
