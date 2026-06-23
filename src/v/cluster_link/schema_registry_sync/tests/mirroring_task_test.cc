/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/schema_registry_sync/mirroring_task.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/tests/deps.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace cluster_link::tests {

namespace ppsr = pandaproxy::schema_registry;
namespace srs = cluster_link::schema_registry_sync;

namespace {

static const model::name_t link_name{"test_sr_link"};
constexpr auto tail_interval = 1s;
constexpr auto wait_interval = 5s;

ppsr::stored_schema make_schema(
  const ppsr::context_subject& sub, int32_t version, std::string_view def) {
    return ppsr::stored_schema{
      .schema = ppsr::
        subject_schema{sub, ppsr::schema_definition{ppsr::schema_definition::raw_string{def}, ppsr::schema_type::avro}},
      .version = ppsr::schema_version{version},
      .id = ppsr::schema_id{version},
      .deleted = ppsr::is_deleted::no};
}

model::metadata get_default_metadata() {
    model::metadata metadata{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = model::link_state{}};

    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "https://schema-registry.example.com";
    api.tail_interval = tail_interval;
    // Long enough that a second full sync within a test only happens because a
    // config change forced it, never on the periodic schedule.
    api.full_sync_interval = 1h;
    metadata.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
    return metadata;
}

/// Test-owned source-of-truth that the injected source reader serves from.
struct fake_source_state {
    chunked_vector<ppsr::stored_schema> schemas;
    std::optional<srs::source_error> list_subjects_error;

    void add(const ppsr::context_subject& sub, int32_t version) {
        schemas.push_back(
          make_schema(sub, version, fmt::format("{{\"v\":{}}}", version)));
    }
};

class fake_source_reader final : public srs::source_reader {
public:
    explicit fake_source_reader(fake_source_state* state)
      : _state(state) {}

    ss::future<srs::source_result<chunked_vector<ppsr::context_subject>>>
    list_subjects(ppsr::context ctx, ss::abort_source&) override {
        if (_state->list_subjects_error.has_value()) {
            co_return std::unexpected(*_state->list_subjects_error);
        }
        chunked_hash_set<ppsr::context_subject> seen;
        chunked_vector<ppsr::context_subject> subjects;
        for (const auto& s : _state->schemas) {
            if (s.schema.sub().ctx != ctx) {
                continue;
            }
            if (seen.insert(s.schema.sub()).second) {
                subjects.push_back(s.schema.sub());
            }
        }
        co_return subjects;
    }

    ss::future<srs::source_result<chunked_vector<ppsr::schema_version>>>
    list_subject_versions(
      ppsr::context_subject sub, ss::abort_source&) override {
        chunked_vector<ppsr::schema_version> versions;
        for (const auto& s : _state->schemas) {
            if (s.schema.sub() == sub) {
                versions.push_back(s.version);
            }
        }
        co_return versions;
    }

    ss::future<srs::source_result<ppsr::stored_schema>> read_subject_version(
      ppsr::context_subject sub,
      ppsr::schema_version version,
      ss::abort_source&) override {
        for (const auto& s : _state->schemas) {
            if (s.schema.sub() == sub && s.version == version) {
                co_return s.share();
            }
        }
        co_return std::unexpected(
          srs::source_error{
            .kind = srs::source_error_kind::operation_failed,
            .message = "not found in source"});
    }

private:
    fake_source_state* _state;
};

class fake_source_reader_factory final : public srs::source_reader_factory {
public:
    explicit fake_source_reader_factory(fake_source_state* state)
      : _state(state) {}

    std::unique_ptr<srs::source_reader> create() override {
        return std::make_unique<fake_source_reader>(_state);
    }

private:
    fake_source_state* _state;
};

} // namespace

class mirroring_task_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        co_await _clmtf->get_manager().invoke_on_all([this](manager& m) {
            return m.register_task_factory<srs::mirroring_task_factory>(
              &_registry, &_source_factory);
        });

        fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ::model::node_id self() { return ::model::node_id(0); }

    void lead_schema_registry() {
        fixture()->elect_leader(
          ::model::schema_registry_internal_ntp, self(), ss::this_shard_id());
    }

    void unlead_schema_registry() {
        fixture()->elect_leader(
          ::model::schema_registry_internal_ntp,
          ::model::node_id(1),
          std::nullopt);
    }

    // Seeds the destination registry with one (subject, version).
    void seed_destination(std::string_view subject, int32_t version) {
        _registry
          .import_schema(make_schema(
            ppsr::context_subject::unqualified(subject),
            version,
            fmt::format("{{\"v\":{}}}", version)))
          .get();
    }

    ss::future<bool> wait_for_task_state(model::task_state state) {
        return fixture()->wait_for_report_to_match(
          wait_interval,
          50ms,
          [state](const model::cluster_link_task_status_report& report) {
              const auto* sr = find_sr_status(report);
              return sr != nullptr && sr->task_state == state;
          });
    }

    static const model::task_status_report*
    find_sr_status(const model::cluster_link_task_status_report& report) {
        auto link_it = report.link_reports.find(link_name);
        if (link_it == report.link_reports.end()) {
            return nullptr;
        }
        auto task_it = link_it->second.task_status_reports.find(
          srs::mirroring_task::task_name);
        if (task_it == link_it->second.task_status_reports.end()) {
            return nullptr;
        }
        return &task_it->second;
    }

    // Extracts the Schema Registry status from a task report's detail.
    static const model::schema_registry_sync_status*
    sr_status(const model::task_status_report* report) {
        if (
          report == nullptr || !report->detail.has_value()
          || !report->detail->schema_registry_sync_status.has_value()) {
            return nullptr;
        }
        return &report->detail->schema_registry_sync_status.value();
    }

    ss::future<std::optional<model::schema_registry_sync_status>>
    wait_for_sync_status(
      std::function<bool(const model::schema_registry_sync_status&)> pred) {
        std::optional<model::schema_registry_sync_status> result;
        co_await ::tests::cooperative_spin_wait_with_timeout(
          wait_interval, [this, &pred, &result]() {
              auto report
                = fixture()->get_manager().local().get_task_status_report();
              const auto* sr = sr_status(find_sr_status(report));
              if (sr != nullptr && pred(*sr)) {
                  result = *sr;
                  return true;
              }
              return false;
          });
        co_return result;
    }

    fake_source_state _source_state;
    fake_source_reader_factory _source_factory{&_source_state};
    schema::fake_registry _registry;
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F(mirroring_task_test, populates_source_and_destination_inventory) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);
    _source_state.add(subject, 2);
    seed_destination("payments-value", 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 2);
    EXPECT_EQ(status->inventory.destination_subjects, 1);
    EXPECT_EQ(status->inventory.destination_subject_versions, 1);
    // Nothing is imported yet, so the destination is unchanged.
    EXPECT_EQ(status->totals_since_task_start.subject_versions_changed, 0);
    EXPECT_EQ(status->last_full_sync->errors, 0);
}

TEST_F(mirroring_task_test, source_unavailable_is_unavailable) {
    _source_state.list_subjects_error = srs::source_error{
      .kind = srs::source_error_kind::source_unavailable,
      .message = "source down"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());
}

TEST_F(mirroring_task_test, config_update_forces_full_resync) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // Wait for the first full sync. The default full-sync interval is long, so
    // without a config-triggered re-scan the inventory would not update again
    // soon.
    auto first = wait_for_sync_status([](const auto& s) {
                     return s.last_full_sync.has_value()
                            && s.inventory.selected_source_subjects == 1;
                 }).get();
    ASSERT_TRUE(first.has_value());

    // Change the source, then update the link config. The config change forces
    // a fresh full scan, so the new source is reflected promptly rather than
    // after the (1h) full-sync interval.
    _source_state.add(ppsr::context_subject::unqualified("payments-value"), 1);
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();

    auto second = wait_for_sync_status([](const auto& s) {
                      return s.inventory.selected_source_subjects == 2;
                  }).get();
    ASSERT_TRUE(second.has_value());
}

TEST_F(mirroring_task_test, follows_partition_leadership) {
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);

    // No leadership on the current node: the task should remain stopped.
    unlead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());

    // Acquire leadership: the task should start and become active.
    lead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());

    // Lose leadership: the task should stop again.
    unlead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());

    // A stopped (non-leader) shard must not surface SR status, otherwise its
    // empty default could win the admin aggregation over the real leader.
    auto report = fixture()->get_manager().local().get_task_status_report();
    const auto* task = find_sr_status(report);
    ASSERT_NE(task, nullptr);
    EXPECT_FALSE(task->detail.has_value());
}

} // namespace cluster_link::tests
