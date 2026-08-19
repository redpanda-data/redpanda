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

#include "cluster_link/link_probe.h"
#include "cluster_link/schema_registry_sync/mirroring_task.h"
#include "cluster_link/schema_registry_sync/probe.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/schema_registry_sync/tests/sr_sync_test_fixtures.h"
#include "cluster_link/tests/deps.h"
#include "container/chunked_vector.h"
#include "metrics/metrics.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "test_utils/async.h"
#include "test_utils/metrics.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gmock/gmock.h>

#include <array>

using namespace std::chrono_literals;

// Found by ADL, so gtest prints these in matcher failures instead of dumping
// their bytes. Only the fields the tests here assert on.
namespace cluster_link::model {

void PrintTo(const schema_registry_inventory& inv, std::ostream* os) {
    fmt::format_to(
      std::ostreambuf_iterator<char>{*os},
      "inventory{{source: {} subjects / {} versions, destination: {} / {}}}",
      inv.selected_source_subjects,
      inv.selected_source_subject_versions,
      inv.destination_subjects,
      inv.destination_subject_versions);
}

} // namespace cluster_link::model

namespace cluster_link::tests {

namespace {

static const model::name_t link_name{"test_sr_link"};
constexpr auto tail_interval = 10ms;
constexpr auto wait_interval = 5s;
// Sampled several times per tick so a state change is seen promptly.
constexpr auto status_poll_interval = 1ms;

// Reads one of the probe's counter series for the test link on the current
// shard (the shard leading `_schemas/0` in these tests). `counter` is the
// name suffix after "schema_registry_"; `handle` selects the internal or
// public registry. nullopt when the series is not registered.
std::optional<uint64_t> sr_sync_metric(std::string_view counter, int handle) {
    return test_utils::find_metric_value<uint64_t>(
      fmt::format(
        "{}_schema_registry_{}", link_probe::shadow_link_group, counter),
      handle,
      {{link_probe::shadow_link_name.name(), link_name()}});
}

const auto both_metric_handles = std::to_array(
  {ss::metrics::default_handle(), metrics::public_metrics_handle});

// A converged mirror: both sides count the same subjects and versions.
model::schema_registry_inventory
converged(uint64_t subjects, uint64_t versions) {
    return {
      .selected_source_subjects = subjects,
      .selected_source_subject_versions = versions,
      .destination_subjects = subjects,
      .destination_subject_versions = versions};
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

// Matches a stored destination version belonging to `subject`.
auto stored_for(const ppsr::context_subject& subject) {
    return testing::Field(
      &ppsr::stored_schema::schema,
      testing::Property(&ppsr::subject_schema::sub, subject));
}

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
              dest(), &_source_factory, &_tail_factory);
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
          status_poll_interval,
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

    // The status as reported right now, for an assertion that nothing changed
    // (which no wait_for_sync_status predicate can express).
    model::schema_registry_sync_status current_sync_status() {
        auto report = fixture()->get_manager().local().get_task_status_report();
        const auto* sr = sr_status(find_sr_status(report));
        return sr == nullptr ? model::schema_registry_sync_status{} : *sr;
    }

    // The destination registry the task writes to. Overridable so a test can
    // wrap `_registry` (e.g. to reject deletes); defaults to `_registry`.
    virtual schema::registry* dest() { return &_registry; }

    // Waits for a full sync to complete, so a change made afterwards can only
    // reach the destination via a tail sync: the test link's full-sync interval
    // is an hour, and only tail ticks follow.
    ss::future<model::schema_registry_sync_status> wait_for_first_full_sync() {
        auto status = co_await wait_for_sync_status([](const auto& s) {
            return s.last_full_sync.has_value() && !s.current_sync.has_value();
        });
        co_return status.value_or(model::schema_registry_sync_status{});
    }

    // Stages `batch` for the next tail poll.
    void stage_tail(srs::tail_batch batch) {
        _tail_state.batches.push_back(std::move(batch));
    }

    // Stages `batch` for every tail poll, so a state it induces holds instead
    // of lasting one tick and being missed by the status waiter.
    void stage_sticky_tail(srs::tail_batch batch) {
        _tail_state.sticky = std::move(batch);
    }

    static srs::tail_batch
    subjects_changed(std::initializer_list<ppsr::context_subject> subjects) {
        srs::tail_batch batch;
        for (const auto& subject : subjects) {
            batch.subjects.insert(subject);
        }
        return batch;
    }

    static srs::tail_batch
    mode_configs_changed(std::initializer_list<ppsr::context_subject> targets) {
        srs::tail_batch batch;
        for (const auto& target : targets) {
            batch.mode_configs.insert(target);
        }
        return batch;
    }

    fake_source_state _source_state;
    fake_source_reader_factory _source_factory{&_source_state};
    fake_tail_state _tail_state;
    fake_tail_reader_factory _tail_factory{&_tail_state};
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
    // The destination counters are refreshed after the sync, so they reflect
    // the post-sync state. The seeded payments-value is absent from the source,
    // so hard-delete propagation purges it; only the two imported orders-value
    // versions remain.
    EXPECT_EQ(status->inventory.destination_subjects, 1);
    EXPECT_EQ(status->inventory.destination_subject_versions, 2);
    // Two orders-value versions imported plus the source-absent payments-value
    // hard-deleted: three subject-version changes.
    EXPECT_EQ(status->totals_since_task_start.subject_versions_changed, 3);
    EXPECT_EQ(status->last_full_sync->errors, 0);
}

TEST_F(mirroring_task_test, full_sync_imports_and_reports) {
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    auto c = ppsr::context_subject::unqualified("c");
    // a:v1 (no refs), b:v1 refs a:v1 (a small ref graph), c:v1 (no refs). The
    // engine must import a before b regardless of listing order.
    _source_state.add(a, 1);
    _source_state.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));
    _source_state.add(c, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && s.last_full_sync->subject_versions_changed == 3
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 3);
    EXPECT_EQ(status->last_full_sync->errors, 0);
    EXPECT_EQ(status->totals_since_task_start.subject_versions_changed, 3);
    // The cumulative summary's start time is stamped once on the task's first
    // run; it was previously left unset (only current_sync carried a start).
    EXPECT_TRUE(status->totals_since_task_start.start_time.has_value());

    // Create-only replication imports schema versions but never touches
    // compatibility configs, subject modes, or unsupported-feature handling,
    // so those deferred counters stay zero.
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 0);
    EXPECT_EQ(status->last_full_sync->modes_changed, 0);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 0);

    // Three source subjects (a, b, c) with one version each; the destination
    // was empty before the sync and, after the post-import refresh, mirrors all
    // three.
    EXPECT_EQ(status->inventory.selected_source_subjects, 3);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 3);
    EXPECT_EQ(status->inventory.destination_subjects, 3);
    EXPECT_EQ(status->inventory.destination_subject_versions, 3);

    // The sync has finished: current_sync is cleared, and last_full_sync
    // carries both a start and a finish timestamp (start <= finish).
    EXPECT_FALSE(status->current_sync.has_value());
    ASSERT_TRUE(status->last_full_sync->start_time.has_value());
    ASSERT_TRUE(status->last_full_sync->finish_time.has_value());
    EXPECT_LE(
      status->last_full_sync->start_time->value(),
      status->last_full_sync->finish_time->value());

    // All three source versions landed on the destination, referent-first.
    const auto& all = _registry.get_all();
    EXPECT_EQ(all.size(), 3);
    EXPECT_LT(index_of(all, "a"), index_of(all, "b"));
}

TEST_F(mirroring_task_test, remove_policy_strips_and_counts_unsupported) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    _source_state.set_unsupported(a, 1, {{.json_pointer = "/ruleSet"}});

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    // The supported projection is imported and the removed feature is counted.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 1);
    EXPECT_EQ(status->totals_since_task_start.unsupported_features_removed, 1);
    EXPECT_GE(index_of(_registry.get_all(), "a"), 0);
}

TEST_F(mirroring_task_test, fail_policy_counts_unsupported_and_syncs_rest) {
    // Under FAIL an unsupported feature is a counted per-item error, not a
    // whole-sync abort: the offending subject is skipped while the rest sync,
    // and the task stays active. Fail-fast is reserved for global errors like
    // source unavailability.
    auto a = ppsr::context_subject::unqualified("a"); // unsupported -> skipped
    auto b = ppsr::context_subject::unqualified("b"); // clean -> imported
    _source_state.add(a, 1);
    _source_state.add(b, 1);
    _source_state.set_unsupported(a, 1, {{.json_pointer = "/ruleSet"}});

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::fail;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    // The full sync completes (the task does not fault): the unsupported
    // subject is counted as an error and skipped, and the clean subject is
    // imported.
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 0);
    EXPECT_EQ(index_of(_registry.get_all(), "a"), -1);
    EXPECT_GE(index_of(_registry.get_all(), "b"), 0);
}

TEST_F(mirroring_task_test, remove_policy_counts_unsupported_config) {
    // The config path applies the same policy as the schema path. Under REMOVE,
    // an unsupported config field (e.g. defaultRuleSet) is counted and logged;
    // only the supported projection (compatibilityLevel) is synced.
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    _source_state.configs.emplace(a, ppsr::compatibility_level::full);
    _source_state.set_config_unsupported(
      a, {{.json_pointer = "/defaultRuleSet"}});

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 1);
    EXPECT_EQ(status->totals_since_task_start.unsupported_features_removed, 1);
    // The compatibility level is still synced under REMOVE.
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
    ASSERT_TRUE(_registry.configs().contains(a));
    EXPECT_EQ(_registry.configs().at(a), ppsr::compatibility_level::full);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    // A second full sync re-reads and re-drops the same fields: the count is
    // per completed sync (mirroring FAIL's per-sync errors), so the total
    // advances even though the config write itself is a no-op.
    const auto first_start = status->last_full_sync->start_time;
    auto metadata2 = get_default_metadata();
    metadata2.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;
    fixture()->upsert_link(std::move(metadata2)).get();
    auto second = wait_for_sync_status([&](const auto& s) {
                      return s.last_full_sync.has_value()
                             && s.last_full_sync->start_time != first_start
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->totals_since_task_start.unsupported_features_removed, 2);
    EXPECT_EQ(second->last_full_sync->unsupported_features_removed, 1);
    EXPECT_EQ(second->totals_since_task_start.errors, 0);
}

TEST_F(mirroring_task_test, remove_policy_counts_unsupported_config_no_level) {
    // A governance-only source config (unsupported fields, no compatibility
    // override): the destination write is a no-op delete, but the dropped
    // fields are still counted -- REMOVE must not ignore them silently.
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    _source_state.set_config_unsupported(
      a, {{.json_pointer = "/compatibilityGroup"}});

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);
    // No compatibility override lands on the destination.
    EXPECT_FALSE(_registry.configs().contains(a));
}

TEST_F(mirroring_task_test, fail_policy_skips_unsupported_config) {
    // Under FAIL, an unsupported config field is a counted per-item error and
    // the subject's config is not synced; the rest of the sync proceeds and the
    // task stays active (the clean schema still imports).
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    _source_state.configs.emplace(a, ppsr::compatibility_level::full);
    _source_state.set_config_unsupported(
      a, {{.json_pointer = "/defaultRuleSet"}});

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::fail;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 0);
    // The config write is skipped: no compatibility change, no destination
    // config.
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 0);
    EXPECT_FALSE(_registry.configs().contains(a));
    // The clean schema itself still imports.
    EXPECT_GE(index_of(_registry.get_all(), "a"), 0);
}

// Fixture whose destination wraps `_registry` so a test can make write_config
// fail, exercising the REMOVE no-count-on-failed-write path the plain fake
// cannot.
class mirroring_task_config_write_failure_test : public mirroring_task_test {
protected:
    schema::registry* dest() override { return &_failing_config; }
    failing_config_registry _failing_config{&_registry};
};

TEST_F(
  mirroring_task_config_write_failure_test,
  remove_policy_does_not_count_unsupported_config_on_write_failure) {
    // REMOVE counts a removed config feature only after the config write lands;
    // a write that fails is a per-item error and must not report the feature as
    // removed (mirrors the schema-body path's no-count-on-failed-import).
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    _source_state.configs.emplace(a, ppsr::compatibility_level::full);
    _source_state.set_config_unsupported(
      a, {{.json_pointer = "/defaultRuleSet"}});
    _failing_config.fail_config(
      a, ppsr::error_code::schema_invalid, "config write rejected");

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()->feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    // The failed write is counted as an error, not as a removed feature.
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->last_full_sync->unsupported_features_removed, 0);
}

TEST_F(mirroring_task_test, source_unavailable_then_recovers) {
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);
    _source_state.list_subjects_error = srs::source_error{
      .kind = srs::source_error_kind::source_unavailable,
      .message = "source down"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // An unreachable source parks the task as link_unavailable.
    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());

    // Once the source recovers, the next tick re-attempts the still-due full
    // sync (the unavailable run left the timer unadvanced) and reaches active.
    _source_state.list_subjects_error.reset();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && s.inventory.selected_source_subjects == 1;
                  }).get();
    ASSERT_TRUE(status.has_value());
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

TEST_F(mirroring_task_test, source_list_failure_completes_and_advances) {
    auto ok = ppsr::context_subject::unqualified("orders-value");
    auto failing = ppsr::context_subject::unqualified("payments-value");
    _source_state.add(ok, 1);
    _source_state.add(failing, 1);
    // Listing payments-value's versions fails (reachable, not unavailable).
    // This is a rare source-side delete race: it is counted as a per-item error
    // and skipped, but the full sync still completes and advances the timer.
    _source_state.list_versions_errors.emplace(
      failing,
      srs::source_error{
        .kind = srs::source_error_kind::operation_failed,
        .message = "version listing failed"});

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    // The full sync completes best-effort: the error is counted, orders-value's
    // single version is listed and imported, and last_full_sync is recorded
    // (the timer advanced -- the failure does not force a fast retry).
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_GE(status->totals_since_task_start.errors, 1);
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);

    // The reachable subject was still imported; the failing one was skipped.
    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().sub(), ppsr::subject{"orders-value"});
}

TEST_F(
  mirroring_task_test, source_context_listing_failure_counts_and_recovers) {
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);
    // A reachable failure (not source_unavailable, which would park the link)
    // must still be counted while the task stays active.
    _source_state.list_contexts_error = srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "context listing failed"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto failed = wait_for_sync_status([](const auto& s) {
                      return s.totals_since_task_start.errors >= 1;
                  }).get();
    ASSERT_TRUE(failed.has_value());
    EXPECT_FALSE(failed->last_full_sync.has_value());
    EXPECT_EQ(failed->last_error_message, "context listing failed");

    _source_state.list_contexts_error.reset();
    auto ok = wait_for_sync_status([](const auto& s) {
                  return s.last_full_sync.has_value()
                         && s.inventory.selected_source_subjects == 1;
              }).get();
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->last_full_sync->subject_versions_changed, 1);
}

TEST_F(mirroring_task_test, source_filter_scopes_discovery_and_import) {
    // The source has two default-context subjects; the configured subject
    // filter selects only orders-value. The excluded payments-value must be
    // neither listed (selected_source_*) nor imported (destination).
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto payments = ppsr::context_subject::unqualified("payments-value");
    _source_state.add(orders, 1);
    _source_state.add(payments, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.subjects.push_back("orders-value");

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    // Only the in-scope subject is selected and imported.
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().sub(), ppsr::subject{"orders-value"});
}

TEST_F(mirroring_task_test, source_filter_excludes_unlisted_context) {
    // The source has a default-context subject and a non-default-context
    // subject. Filtering to the default context must exclude the non-default
    // one from discovery -- and, because it is excluded, the run never needs
    // qualified subjects for it.
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto other = ppsr::context_subject{
      ppsr::context{".other"}, ppsr::subject{"x"}};
    _source_state.contexts.push_back(ppsr::context{".other"});
    _source_state.add(orders, 1);
    _source_state.add(other, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.contexts.push_back(std::string{ppsr::default_context()});

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();

    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->inventory.selected_source_subjects, 1);
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 1);
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub().ctx, ppsr::default_context);
}

TEST_F(mirroring_task_test, syncs_soft_deleted_source_versions) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto payments = ppsr::context_subject::unqualified("payments-value");
    // orders-value: v1 active, v2 soft-deleted. payments-value: only a
    // soft-deleted v1 (no active version at all, so it is reached only because
    // discovery enumerates deleted versions).
    _source_state.add(orders, 1);
    _source_state.add(orders, 2, ppsr::is_deleted::yes);
    _source_state.add(payments, 1, ppsr::is_deleted::yes);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // The selected-versions count spans active and soft-deleted discoveries
    // (orders v1 + v2, payments v1), not just the active subset.
    EXPECT_EQ(status->inventory.selected_source_subject_versions, 3);

    // All three versions are synced, each preserving its source deleted state.
    const auto& all = _registry.get_all();
    EXPECT_EQ(all.size(), 3);
    auto find_ver = [&](
                      const ppsr::context_subject& sub,
                      int32_t v) -> const ppsr::stored_schema* {
        for (const auto& s : all) {
            if (s.schema.sub() == sub && s.version == ppsr::schema_version{v}) {
                return &s;
            }
        }
        return nullptr;
    };
    const auto* o1 = find_ver(orders, 1);
    const auto* o2 = find_ver(orders, 2);
    const auto* p1 = find_ver(payments, 1);
    ASSERT_NE(o1, nullptr);
    ASSERT_NE(o2, nullptr);
    ASSERT_NE(p1, nullptr);
    EXPECT_EQ(o1->deleted, ppsr::is_deleted::no);
    EXPECT_EQ(o2->deleted, ppsr::is_deleted::yes);
    EXPECT_EQ(p1->deleted, ppsr::is_deleted::yes);
}

TEST_F(
  mirroring_task_test, propagates_source_soft_delete_to_active_destination) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // The destination holds orders-value:v1 active (a prior sync imported it);
    // the source has since soft-deleted it. The run must propagate the delete
    // by re-importing the deleted body over the live destination version.
    seed_destination("orders-value", 1);
    _source_state.add(orders, 1, ppsr::is_deleted::yes);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status
      = wait_for_sync_status([](const auto& s) {
            return s.last_full_sync.has_value() && !s.current_sync.has_value()
                   && s.totals_since_task_start.subject_versions_changed >= 1;
        }).get();
    ASSERT_TRUE(status.has_value());

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub(), orders);
    EXPECT_EQ(all[0].version, ppsr::schema_version{1});
    EXPECT_EQ(all[0].deleted, ppsr::is_deleted::yes);
}

TEST_F(mirroring_task_test, hard_deletes_source_absent_versions) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto payments = ppsr::context_subject::unqualified("payments-value");
    // The destination has two active subjects; the source only has
    // orders-value. The source-absent payments-value must be soft-deleted then
    // hard-deleted, while orders-value is kept.
    seed_destination("orders-value", 1);
    seed_destination("payments-value", 1);
    _source_state.add(orders, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // orders-value was already present (no import); the one change is the
    // payments-value hard-delete.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    EXPECT_EQ(all[0].schema.sub(), orders);
}

TEST_F(
  mirroring_task_test,
  out_of_scope_destination_subject_spared_from_hard_delete) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // Both subjects sit in the default context on the destination, but the
    // subject filter selects only orders-value, so payments-value is out of
    // scope. It is absent from the scoped source view -- yet must not be read
    // as a source hard-delete.
    seed_destination("orders-value", 1);
    seed_destination("payments-value", 1);
    _source_state.add(orders, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.subjects.push_back("orders-value");

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // orders-value is already present (no import) and payments-value is out of
    // scope (no purge), so the sync changes nothing.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 0);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    // Both destination subjects survive: the in-scope one untouched, the
    // out-of-scope one spared rather than purged.
    const auto& all = _registry.get_all();
    chunked_vector<ppsr::subject> subjects;
    for (const auto& s : all) {
        subjects.push_back(s.schema.sub().sub);
    }
    EXPECT_THAT(
      subjects,
      testing::UnorderedElementsAre(
        ppsr::subject{"orders-value"}, ppsr::subject{"payments-value"}));
}

TEST_F(mirroring_task_test, hard_deletes_already_soft_deleted_version) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // The destination holds orders-value:v1 already soft-deleted; the source no
    // longer has it at all, so it is purged (directly, no re-soft-delete).
    _registry
      .import_schema(
        make_schema(orders, 1, R"({"v":1})", ppsr::is_deleted::yes))
      .get();

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_TRUE(_registry.get_all().empty());
}

TEST_F(mirroring_task_test, unlisted_subject_spared_from_hard_delete) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    auto flaky = ppsr::context_subject::unqualified("payments-value");
    // Both subjects were mirrored on the destination by a prior sync. The
    // source still has both, but listing flaky's versions fails transiently
    // (reachable, not unavailable), so it drops out of the discovered source
    // set and would look source-absent to the purge phase.
    seed_destination("orders-value", 1);
    seed_destination("payments-value", 1);
    _source_state.add(orders, 1);
    _source_state.add(flaky, 1);
    _source_state.list_versions_errors.emplace(
      flaky,
      srs::source_error{
        .kind = srs::source_error_kind::operation_failed,
        .message = "version listing failed"});

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->errors, 1);

    // The unlistable subject is excluded from the purge, so it survives rather
    // than being erased on a discovery gap; nothing is hard-deleted.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 0);
    EXPECT_EQ(_registry.get_all().size(), 2u);
}

TEST_F(mirroring_task_test, deleted_subject_purged_despite_unlisted_peer) {
    auto flaky = ppsr::context_subject::unqualified("payments-value");
    auto gone = ppsr::context_subject::unqualified("orders-value");
    // The destination mirrors two subjects. The source still has flaky (but its
    // version listing fails transiently) and no longer has gone at all. The
    // failed peer must not block purging gone: discovery saw the source lacks
    // it, so it is a real deletion.
    seed_destination("payments-value", 1);
    seed_destination("orders-value", 1);
    _source_state.add(flaky, 1);
    _source_state.list_versions_errors.emplace(
      flaky,
      srs::source_error{
        .kind = srs::source_error_kind::operation_failed,
        .message = "version listing failed"});

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->errors, 1);

    // gone is purged (one change); the unlistable flaky is spared and remains.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0].schema.sub(), flaky);
}

TEST_F(mirroring_task_test, failed_context_listing_spares_its_subjects) {
    auto kept = ppsr::context_subject::unqualified("orders-value");
    // The in-scope context's subject listing fails reachably (not unavailable,
    // which would park the link), so its whole subject set is undiscovered. A
    // destination subject in that context must be spared the purge rather than
    // treated as source-absent.
    seed_destination("orders-value", 1);
    _source_state.list_subjects_error = srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "subject listing failed"};

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->errors, 1);

    // The whole context was spared, so its seeded subject survives and nothing
    // is hard-deleted.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 0);
    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0].schema.sub(), kept);
}

TEST_F(mirroring_task_test, replicates_modes_and_configs) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(orders, 1);
    // A subject-level mode and compatibility override on the source.
    _source_state.modes.emplace(orders, ppsr::mode::read_only);
    _source_state.configs.emplace(orders, ppsr::compatibility_level::full);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // Exactly one mode and one config change: the subject override. The
    // context-level and global targets have no source override, so their
    // no-op deletes do not count.
    EXPECT_EQ(status->last_full_sync->modes_changed, 1);
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    ASSERT_TRUE(_registry.modes().contains(orders));
    EXPECT_EQ(_registry.modes().at(orders), ppsr::mode::read_only);
    ASSERT_TRUE(_registry.configs().contains(orders));
    EXPECT_EQ(_registry.configs().at(orders), ppsr::compatibility_level::full);

    // A second full sync over unchanged source state applies nothing: the
    // destination writes short-circuit, so the per-sync counters stay zero
    // (the totals do not double-count).
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();
    auto second = wait_for_sync_status([&](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value()
                             && s.last_full_sync->finish_time
                                  != status->last_full_sync->finish_time;
                  }).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->last_full_sync->modes_changed, 0);
    EXPECT_EQ(second->last_full_sync->compatibility_configs_changed, 0);
    EXPECT_EQ(second->totals_since_task_start.modes_changed, 1);
    EXPECT_EQ(second->totals_since_task_start.compatibility_configs_changed, 1);
}

TEST_F(mirroring_task_test, removes_destination_override_absent_at_source) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // The destination carries mode/config overrides from a prior state; the
    // source subject exists but has no explicit override. The sync must remove
    // the stale destination overrides.
    seed_destination("orders-value", 1);
    _registry.write_mode(orders, ppsr::mode::read_only).get();
    _registry.write_config(orders, ppsr::compatibility_level::full).get();
    _source_state.add(orders, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    EXPECT_EQ(status->last_full_sync->modes_changed, 1);
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
    EXPECT_FALSE(_registry.modes().contains(orders));
    EXPECT_FALSE(_registry.configs().contains(orders));
}

TEST_F(mirroring_task_test, unmappable_source_mode_counted_not_applied) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(orders, 1);
    // The source reports a mode Redpanda cannot represent (the http reader
    // surfaces this as operation_failed); it must be counted as a per-item
    // error and skipped, not applied to the destination.
    _source_state.read_mode_errors.emplace(
      orders,
      srs::source_error{
        .kind = srs::source_error_kind::operation_failed,
        .message = "source mode 'FORWARD' is not supported"});

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // Exactly one error: the single unmappable mode. Every other target (the
    // subject's config, the default-context mode/config) is a no-op.
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(status->last_full_sync->modes_changed, 0);
    // The unmappable mode does not block the subject's config sync, which has
    // no source override here and so applies nothing.
    EXPECT_FALSE(_registry.modes().contains(orders));
}

TEST_F(mirroring_task_test, syncs_global_context_mode_and_config) {
    // An unfiltered sync mirrors the registry-wide global (.__GLOBAL)
    // mode/config, even though it is not a listable context and holds no
    // subject.
    auto global = ppsr::global_mode_config_target;
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);
    _source_state.modes.emplace(global, ppsr::mode::read_only);
    _source_state.configs.emplace(global, ppsr::compatibility_level::full);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    ASSERT_TRUE(_registry.modes().contains(global));
    EXPECT_EQ(_registry.modes().at(global), ppsr::mode::read_only);
    ASSERT_TRUE(_registry.configs().contains(global));
    EXPECT_EQ(_registry.configs().at(global), ppsr::compatibility_level::full);
    // Exactly the global mode + global config change (no subject/context
    // override in this DAG).
    EXPECT_EQ(status->last_full_sync->modes_changed, 1);
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
}

TEST_F(mirroring_task_test, skips_global_context_mode_when_filtered_out) {
    // A link scoped to a specific context must leave the registry-wide global
    // (.__GLOBAL) alone -- it is synced only by an unfiltered sync or a filter
    // that names the global context.
    auto global = ppsr::global_mode_config_target;
    auto orders = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(orders, 1);
    _source_state.modes.emplace(global, ppsr::mode::read_only);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.contexts.push_back(std::string{ppsr::default_context()});

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // The source global override is neither read nor written.
    EXPECT_FALSE(_registry.modes().contains(global));
    EXPECT_EQ(status->last_full_sync->modes_changed, 0);
}

TEST_F(mirroring_task_test, replicates_context_level_mode_and_config) {
    // A context-level override -- a whole context's default mode/config, keyed
    // by the empty subject -- mirrors like a subject override, through the
    // per-context targets of the mode/config pass. It is distinct from a
    // subject in the context and from the registry-wide global (.__GLOBAL).
    // Identity mapping, so the override stays under its own (.prod) context.
    auto prod_ctx = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{""}};
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(
      ppsr::context_subject{
        ppsr::context{".prod"}, ppsr::subject{"orders-value"}},
      1);
    _source_state.modes.emplace(prod_ctx, ppsr::mode::read_only);
    _source_state.configs.emplace(prod_ctx, ppsr::compatibility_level::full);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());

    // Exactly one context-level mode + config change: the subject, the default
    // context, and the global target carry no override, so their no-op deletes
    // do not count.
    EXPECT_EQ(status->last_full_sync->modes_changed, 1);
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    ASSERT_TRUE(_registry.modes().contains(prod_ctx));
    EXPECT_EQ(_registry.modes().at(prod_ctx), ppsr::mode::read_only);
    ASSERT_TRUE(_registry.configs().contains(prod_ctx));
    EXPECT_EQ(
      _registry.configs().at(prod_ctx), ppsr::compatibility_level::full);
}

TEST_F(mirroring_task_test, remaps_source_context_to_destination) {
    // Collapse the source .prod context onto the destination default context.
    // Mapping to the default target keeps the test independent of the
    // qualified-subjects cluster config (a non-default destination would need
    // it enabled). Filter to .prod so the mapping fully covers the scope.
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(prod_orders, 1);
    _source_state.modes.emplace(prod_orders, ppsr::mode::read_only);
    _source_state.configs.emplace(prod_orders, ppsr::compatibility_level::full);

    auto metadata = get_default_metadata();
    auto* api = metadata.configuration.schema_registry_sync_cfg.api_mode();
    api->filter.contexts.push_back(".prod");
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", std::string{ppsr::default_context()});
    api->destination = std::move(mapping);

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    // The schema lands in the destination's default context (remapped from
    // .prod), not in .prod.
    const auto& all = _registry.get_all();
    ASSERT_EQ(all.size(), 1);
    auto dest_orders = ppsr::context_subject::unqualified("orders-value");
    EXPECT_EQ(all[0].schema.sub(), dest_orders);

    // Mode and compatibility are written under the remapped (default) context.
    ASSERT_TRUE(_registry.modes().contains(dest_orders));
    EXPECT_EQ(_registry.modes().at(dest_orders), ppsr::mode::read_only);
    ASSERT_TRUE(_registry.configs().contains(dest_orders));
    EXPECT_EQ(
      _registry.configs().at(dest_orders), ppsr::compatibility_level::full);

    // The destination scan reverse-maps the default context back to .prod, so
    // the mirrored subject is recognised as in-scope rather than hard-deleted.
    EXPECT_EQ(status->inventory.destination_subjects, 1);
    EXPECT_EQ(status->inventory.destination_subject_versions, 1);
}

TEST_F(mirroring_task_test, remaps_context_level_mode_and_config) {
    // A context-level override on a source context is written under the
    // REMAPPED destination context, not the source one -- the context-level
    // counterpart of remaps_source_context_to_destination. Remaps .prod onto a
    // distinct .staging context; both are non-default, which the on-by-default
    // schema_registry_enable_qualified_subjects permits.
    auto prod_ctx = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{""}};
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(prod_orders, 1);
    _source_state.modes.emplace(prod_ctx, ppsr::mode::read_only);
    _source_state.configs.emplace(prod_ctx, ppsr::compatibility_level::full);

    auto metadata = get_default_metadata();
    auto* api = metadata.configuration.schema_registry_sync_cfg.api_mode();
    api->filter.contexts.push_back(".prod");
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", ".staging");
    api->destination = std::move(mapping);

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->modes_changed, 1);
    EXPECT_EQ(status->last_full_sync->compatibility_configs_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 0);

    // The override lands on the remapped .staging context, keyed by the empty
    // subject; nothing is written under the source .prod context.
    auto dest_ctx = ppsr::context_subject{
      ppsr::context{".staging"}, ppsr::subject{""}};
    ASSERT_TRUE(_registry.modes().contains(dest_ctx));
    EXPECT_EQ(_registry.modes().at(dest_ctx), ppsr::mode::read_only);
    ASSERT_TRUE(_registry.configs().contains(dest_ctx));
    EXPECT_EQ(
      _registry.configs().at(dest_ctx), ppsr::compatibility_level::full);
    EXPECT_FALSE(_registry.modes().contains(prod_ctx));
    EXPECT_FALSE(_registry.configs().contains(prod_ctx));
}

TEST_F(mirroring_task_test, pauses_when_config_paused) {
    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());

    // Pausing the config disables the task. It still leads _schemas/0, so the
    // reconciler pauses it (rather than stopping it, which is
    // placement-driven).
    auto paused = get_default_metadata();
    paused.configuration.schema_registry_sync_cfg.api_mode()->is_enabled
      = model::enabled_t::no;
    fixture()->update_link(model::id_t{0}, std::move(paused)).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::paused).get());

    // Un-pausing re-enables the task; the reconciler brings it back to active.
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
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

TEST_F(mirroring_task_test, exports_sync_totals_on_both_metric_endpoints) {
    // Three subject versions imported by the first full sync; the probe's
    // counters must mirror totals_since_task_start on both the internal and
    // the public registry, labelled with the link name.
    _source_state.add(ppsr::context_subject::unqualified("a"), 1);
    _source_state.add(ppsr::context_subject::unqualified("b"), 1);
    _source_state.add(ppsr::context_subject::unqualified("c"), 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    ASSERT_EQ(status->totals_since_task_start.subject_versions_changed, 3);

    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{3});
        // The other counters have not moved, but their series exist.
        EXPECT_EQ(
          sr_sync_metric("compatibility_configs_changed", handle),
          std::optional<uint64_t>{0});
        EXPECT_EQ(
          sr_sync_metric("modes_changed", handle), std::optional<uint64_t>{0});
        EXPECT_EQ(
          sr_sync_metric("unsupported_features_removed", handle),
          std::optional<uint64_t>{0});
        EXPECT_EQ(sr_sync_metric("errors", handle), std::optional<uint64_t>{0});
    }
}

TEST_F(mirroring_task_test, metric_series_survive_pause_and_drop_on_stop) {
    _source_state.add(ppsr::context_subject::unqualified("orders-value"), 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.totals_since_task_start.subject_versions_changed
                             == 1;
                  }).get();
    ASSERT_TRUE(status.has_value());

    // Pausing (config-disabled while still leading) keeps the task's totals,
    // so the series must stay registered and keep their values.
    auto paused = get_default_metadata();
    paused.configuration.schema_registry_sync_cfg.api_mode()->is_enabled
      = model::enabled_t::no;
    fixture()->update_link(model::id_t{0}, std::move(paused)).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::paused).get());
    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{1});
    }

    // Resuming re-enters start(), whose probe setup must be idempotent.
    fixture()->update_link(model::id_t{0}, get_default_metadata()).get();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{1});
    }

    // Losing `_schemas/0` leadership stops the task; a stopped task's totals
    // are reset, so a lingering series would export misleading zeros -- the
    // series must be removed outright. The state flips to stopped before the
    // runner drains and the probe clears, so poll rather than assert.
    unlead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());
    for (auto handle : both_metric_handles) {
        ::tests::cooperative_spin_wait_with_timeout(wait_interval, [handle] {
            return !sr_sync_metric("subject_versions_changed", handle)
                      .has_value();
        }).get();
    }
}

TEST_F(mirroring_task_test, totals_reset_across_leadership_tenures) {
    // stop() resets the sync state after dropping the series; a task that
    // regains `_schemas/0` leadership (A->B->A) re-registers the series and
    // must export the new tenure's totals only. A scrape after the reset must
    // read the reassigned _status, not anything bound at first setup.
    auto subject = ppsr::context_subject::unqualified("orders-value");
    _source_state.add(subject, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto first = wait_for_sync_status([](const auto& s) {
                     return s.totals_since_task_start.subject_versions_changed
                            == 1;
                 }).get();
    ASSERT_TRUE(first.has_value());

    unlead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());

    // v2 appears while not leading; the second tenure's first full sync
    // imports only the missing version, so its totals are exactly 1.
    _source_state.add(subject, 2);
    lead_schema_registry();
    auto second = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->totals_since_task_start.subject_versions_changed, 1);

    // 2 would mean the first tenure's totals leaked through the reset.
    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{1});
    }
}

TEST_F(mirroring_task_test, metric_values_are_fetched_live_on_scrape) {
    model::schema_registry_sync_summary totals;
    srs::probe probe;
    probe.setup(link_name, [&totals] { return totals; });

    const auto counters = std::to_array<std::pair<std::string_view, uint64_t>>(
      {{"subject_versions_changed", 1},
       {"compatibility_configs_changed", 2},
       {"modes_changed", 3},
       {"unsupported_features_removed", 4},
       {"errors", 5}});

    for (auto handle : both_metric_handles) {
        for (const auto& counter : counters) {
            EXPECT_EQ(
              sr_sync_metric(counter.first, handle),
              std::optional<uint64_t>{0});
        }
    }

    totals.subject_versions_changed = 1;
    totals.compatibility_configs_changed = 2;
    totals.modes_changed = 3;
    totals.unsupported_features_removed = 4;
    totals.errors = 5;
    for (auto handle : both_metric_handles) {
        for (const auto& [name, value] : counters) {
            EXPECT_EQ(
              sr_sync_metric(name, handle), std::optional<uint64_t>{value});
        }
    }

    probe.clear();
}

// Fixture whose destination parks an import mid-reconcile, so a test can
// observe the task while _reconcile_stats holds counts not yet folded into
// _status.
class mirroring_task_blocking_import_test : public mirroring_task_test {
public:
    // A parked import holds the runner's gate; the task stop in the base
    // teardown would wait on it forever, so release first.
    ss::future<> TearDownAsync() override {
        release();
        co_await mirroring_task_test::TearDownAsync();
    }

protected:
    schema::registry* dest() override { return &_blocking; }

    void release() {
        if (!_released) {
            _released = true;
            _blocking.unblock();
        }
    }

    blocking_import_registry _blocking{&_registry, /*block_after=*/1};

private:
    bool _released{false};
};

TEST_F(
  mirroring_task_blocking_import_test,
  metrics_include_in_flight_reconcile_stats) {
    // The reconciler counts each import in _reconcile_stats, which is folded
    // into _status only at end of run. Parking the second import mid-reconcile
    // pins that the exported counters include the in-flight stats. Only
    // EXPECTs between park and release, so the parked import is always
    // released.
    _source_state.add(ppsr::context_subject::unqualified("a"), 1);
    _source_state.add(ppsr::context_subject::unqualified("b"), 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    _blocking.entered().get();
    // entered() races the forwarded import's completion continuation; poll
    // the report (which shares the live fold) until the count shows.
    auto status
      = wait_for_sync_status([](const auto& s) {
            return s.current_sync.has_value()
                   && s.totals_since_task_start.subject_versions_changed == 1;
        }).get();
    EXPECT_TRUE(status.has_value());

    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{1});
    }

    release();
    auto done = wait_for_sync_status([](const auto& s) {
                    return s.last_full_sync.has_value()
                           && !s.current_sync.has_value();
                }).get();
    EXPECT_TRUE(done.has_value());
    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{2});
    }
}

TEST_F(
  mirroring_task_blocking_import_test,
  scrape_during_stop_drain_exports_last_totals) {
    // Losing leadership flips the task to stopped immediately, but the series
    // are only dropped once the runner's gate drains; a parked import wedges
    // stop() in exactly that window. A scrape landing there must still export
    // the last live totals -- reading them through the status report instead
    // would hit the optional that get_status_report() leaves disengaged while
    // stopped.
    _source_state.add(ppsr::context_subject::unqualified("a"), 1);
    _source_state.add(ppsr::context_subject::unqualified("b"), 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    _blocking.entered().get();
    auto status
      = wait_for_sync_status([](const auto& s) {
            return s.current_sync.has_value()
                   && s.totals_since_task_start.subject_versions_changed == 1;
        }).get();
    EXPECT_TRUE(status.has_value());

    unlead_schema_registry();
    ASSERT_TRUE(wait_for_task_state(model::task_state::stopped).get());
    for (auto handle : both_metric_handles) {
        EXPECT_EQ(
          sr_sync_metric("subject_versions_changed", handle),
          std::optional<uint64_t>{1});
    }

    release();
    // The drain completes, stop() drops the series and resets the totals.
    ::tests::cooperative_spin_wait_with_timeout(wait_interval, [] {
        return !sr_sync_metric(
                  "subject_versions_changed", ss::metrics::default_handle())
                  .has_value();
    }).get();
}

TEST_F(mirroring_task_test, inventory_counts_soft_deleted_versions_both_sides) {
    // "a" keeps an active version and a soft-deleted one; "gone" is
    // soft-deleted outright, which the source still lists (its listings are
    // include_deleted) and the sync still imports. Both sides must therefore
    // count soft-deleted versions, or a converged mirror reports fewer than it
    // holds.
    auto a = ppsr::context_subject::unqualified("a");
    auto gone = ppsr::context_subject::unqualified("gone");
    _source_state.add(a, 1);
    _source_state.add(a, 2);
    _source_state.soft_delete(a, 2);
    _source_state.add(gone, 1);
    _source_state.soft_delete(gone, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto status = wait_for_first_full_sync().get();

    EXPECT_THAT(status.inventory, converged(2, 3));
}

TEST_F(mirroring_task_test, destination_inventory_spans_contexts_and_deleted) {
    auto a = ppsr::context_subject::unqualified("a");
    auto c = ppsr::context_subject{ppsr::context{".b"}, ppsr::subject{"c"}};
    // Default-context "a": v1 active, v2 soft-deleted. Context ".b" subject
    // "c": v1 active. The scan must span both contexts and separate active from
    // soft-deleted.
    _registry.import_schema(make_schema(a, 1, R"({"v":1})")).get();
    _registry
      .import_schema(make_schema(a, 2, R"({"v":2})", ppsr::is_deleted::yes))
      .get();
    _registry.import_schema(make_schema(c, 1, R"({"v":1})")).get();

    ss::abort_source as;
    srs::context_mapper identity;
    auto inv = srs::scan_destination_inventory(
                 _registry,
                 [](const ppsr::context_subject&) { return true; },
                 identity,
                 as)
                 .get();

    auto a_v1 = ppsr::subject_version{a, ppsr::schema_version{1}};
    auto a_v2 = ppsr::subject_version{a, ppsr::schema_version{2}};
    auto c_v1 = ppsr::subject_version{c, ppsr::schema_version{1}};

    EXPECT_THAT(inv.active, testing::UnorderedElementsAre(a_v1, c_v1));
    EXPECT_THAT(inv.all, testing::UnorderedElementsAre(a_v1, c_v1, a_v2));
}

TEST_F(mirroring_task_test, deletes_source_absent_context) {
    // The destination holds a subject under .prod, materializing that context;
    // the source has no .prod context at all. The whole context is a deletion:
    // its subject is purged, then the now-empty context is tombstoned so it no
    // longer lists. A default-context subject present at the source is synced
    // and left alone, showing the phase deletes only the source-absent context.
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _registry.import_schema(make_schema(prod_orders, 1, R"({"v":1})")).get();
    auto keep = ppsr::context_subject::unqualified("keep-value");
    _source_state.add(keep, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    // Present, error-free, and exactly two subject-version changes: keep-value
    // imported (+1) and the source-absent .prod subject purged (+1).
    ASSERT_THAT(
      status,
      testing::Optional(
        testing::Field(
          &model::schema_registry_sync_status::last_full_sync,
          testing::Optional(
            testing::AllOf(
              testing::Field(&model::schema_registry_sync_summary::errors, 0),
              testing::Field(
                &model::schema_registry_sync_summary::subject_versions_changed,
                2))))));

    // The .prod subject was purged; only keep-value remains.
    EXPECT_THAT(
      _registry.get_all(),
      testing::ElementsAre(
        testing::Field(
          &ppsr::stored_schema::schema,
          testing::Property(&ppsr::subject_schema::sub, keep))));

    // .prod is tombstoned: only the default context is still materialized.
    EXPECT_THAT(
      _registry.list_contexts().get(),
      testing::ElementsAre(ppsr::default_context));
}

// Fixture whose destination wraps `_registry` so a test can make permanent
// deletes fail, exercising the hard-delete retry loop the plain fake cannot.
class mirroring_task_delete_retry_test : public mirroring_task_test {
protected:
    schema::registry* dest() override { return &_deferring; }
    deferred_delete_registry _deferring{&_registry};
};

TEST_F(
  mirroring_task_delete_retry_test,
  hard_delete_retries_reference_blocked_purge) {
    auto referrer = ppsr::context_subject::unqualified("referrer-value");
    auto referent = ppsr::context_subject::unqualified("referent-value");
    // Both are on the destination but absent from the source, so both are
    // purged. The referent cannot be deleted until the referrer is, so its
    // first attempt is rejected; the sync must delete the referrer and retry
    // the referent in a later round rather than count an error.
    seed_destination("referrer-value", 1);
    seed_destination("referent-value", 1);
    _deferring.block_until_purged(referent, referrer);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    // Both drained within the one sync, no error counted for the retry.
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 2);
    EXPECT_EQ(status->last_full_sync->errors, 0);
    EXPECT_TRUE(_registry.get_all().empty());
}

TEST_F(mirroring_task_delete_retry_test, hard_delete_gives_up_on_stuck_purge) {
    auto orders = ppsr::context_subject::unqualified("orders-value");
    // A purge that never succeeds (a reference cycle the source can't have, but
    // defensively handled): once a whole round makes no progress the sync stops
    // retrying and counts it as one error rather than looping forever.
    seed_destination("orders-value", 1);
    _deferring.reject_forever(orders);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 0);
    EXPECT_EQ(status->last_full_sync->errors, 1);
}

TEST_F(
  mirroring_task_delete_retry_test,
  hard_delete_counts_non_reference_error_without_retry) {
    auto stuck = ppsr::context_subject::unqualified("stuck-value");
    auto ok = ppsr::context_subject::unqualified("ok-value");
    // A non-reference destination fault (writes disabled) must be counted once,
    // immediately -- not deferred and retried like a reference-ordering block.
    // `ok` purges normally, so a retry loop would re-attempt `stuck` in a later
    // round; asserting a single attempt pins the no-retry behavior.
    seed_destination("stuck-value", 1);
    seed_destination("ok-value", 1);
    _deferring.fail_with(stuck, ppsr::error_code::writes_disabled);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->last_full_sync->subject_versions_changed, 1);
    EXPECT_EQ(status->last_full_sync->errors, 1);
    EXPECT_EQ(_deferring.permanent_delete_attempts(stuck), 1);
}

TEST_F(mirroring_task_delete_retry_test, defers_delete_of_non_empty_context) {
    // The .prod context is source-absent, but its only subject's purge is
    // permanently blocked, so the context never empties. delete_context is
    // skipped (context_not_empty) rather than counted as an extra error or
    // faulted, leaving the context listed for a later full sync to retry.
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _registry.import_schema(make_schema(prod_orders, 1, R"({"v":1})")).get();
    _deferring.reject_forever(prod_orders);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.last_full_sync.has_value()
                             && !s.current_sync.has_value();
                  }).get();
    // The single error is the stuck purge; the skipped context delete adds
    // none.
    ASSERT_THAT(
      status,
      testing::Optional(
        testing::Field(
          &model::schema_registry_sync_status::last_full_sync,
          testing::Optional(
            testing::Field(&model::schema_registry_sync_summary::errors, 1)))));

    // .prod is not deleted: it stays listed alongside the default context, so a
    // later full sync retries it. A still-listed non-empty context also implies
    // its blocked subject survived.
    EXPECT_THAT(
      _registry.list_contexts().get(),
      testing::UnorderedElementsAre(
        ppsr::default_context, ppsr::context{".prod"}));
}

TEST_F(mirroring_task_test, tail_sync_imports_a_newly_registered_version) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto full = wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    // Registered after the full sync, so only a tail sync can carry it across.
    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));

    auto status = wait_for_sync_status([](const auto& s) {
                      return s.totals_since_task_start.subject_versions_changed
                             == 2;
                  }).get();

    EXPECT_THAT(_registry.get_all(), testing::SizeIs(2));
    EXPECT_THAT(
      status,
      testing::Optional(
        testing::AllOf(
          // A tail sync is no substitute for a full scan, so it must not report
          // as one: the full-sync summary is still the previous sync's.
          testing::Field(
            &model::schema_registry_sync_status::last_full_sync,
            testing::Optional(
              testing::Field(
                &model::schema_registry_sync_summary::finish_time,
                full.last_full_sync->finish_time))),
          // Nor may it rewrite the source inventory, which describes the whole
          // selected source rather than the subjects one tick examined.
          testing::Field(
            &model::schema_registry_sync_status::inventory,
            testing::AllOf(
              testing::Field(
                &model::schema_registry_inventory::selected_source_subjects, 1),
              testing::Field(
                &model::schema_registry_inventory::
                  selected_source_subject_versions,
                1))))));
}

TEST_F(mirroring_task_test, tail_sync_hard_deletes_a_removed_subject) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    // The source drops the subject entirely, so its listings report it absent
    // rather than failing -- which is what makes the destination copy
    // purgeable.
    _source_state.remove_subject(a);
    stage_tail(subjects_changed({a}));

    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().empty();
    }).get();
    EXPECT_THAT(_registry.get_all(), testing::IsEmpty());
}

TEST_F(mirroring_task_test, tail_sync_purges_only_the_subjects_it_examined) {
    auto named = ppsr::context_subject::unqualified("named");
    auto unnamed = ppsr::context_subject::unqualified("unnamed");
    _source_state.add(named, 1);
    _source_state.add(unnamed, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(2));

    // Both subjects vanish from the source, but only one is named in the batch.
    // The unnamed one went unexamined this tick, so it must survive: reading
    // "not discovered" as "source-absent" would wipe the mirror one tick after
    // any single change.
    _source_state.remove_subject(named);
    _source_state.remove_subject(unnamed);
    stage_tail(subjects_changed({named}));

    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 1;
    }).get();
    EXPECT_THAT(_registry.get_all(), testing::ElementsAre(stored_for(unnamed)));
}

TEST_F(mirroring_task_test, tail_sync_ignores_out_of_scope_subjects) {
    auto in = ppsr::context_subject::unqualified("in-scope");
    auto out = ppsr::context_subject::unqualified("out-of-scope");
    _source_state.add(in, 1);
    _source_state.add(out, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.subjects.push_back("in-scope");

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    // The reader reports every source change because it knows nothing of the
    // filter, so the task must drop the out-of-scope one.
    _source_state.add(in, 2);
    _source_state.add(out, 2);
    stage_tail(subjects_changed({in, out}));

    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    EXPECT_THAT(
      _registry.get_all(),
      testing::ElementsAre(stored_for(in), stored_for(in)));
}

TEST_F(mirroring_task_test, tail_poll_failure_is_counted_and_recovers) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    _tail_state.poll_error = srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "tail poll failed"};
    auto errored = wait_for_sync_status([](const auto& s) {
                       return s.totals_since_task_start.errors == 1;
                   }).get();
    ASSERT_TRUE(errored.has_value());
    // A failed poll is a counted per-item error, not a parked link.
    EXPECT_TRUE(wait_for_task_state(model::task_state::active).get());

    // Recovery needs no full sync: the next poll simply succeeds.
    _tail_state.poll_error.reset();
    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));
    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(2));
}

TEST_F(
  mirroring_task_test, tail_source_unavailable_replays_the_consumed_change) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));
    const auto listings = _source_state.list_contexts_calls;

    // The tail consumes a real change, but refreshing the named subject finds
    // the source unavailable. The batch goes back to the reader, so a later
    // tick can retry it.
    _source_state.add(a, 2);
    _source_state.list_versions_errors.emplace(
      a,
      srs::source_error{
        .kind = srs::source_error_kind::source_unavailable,
        .message = "source down after tail poll"});
    stage_tail(subjects_changed({a}));

    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());
    EXPECT_GT(_tail_state.rewinds, 0);

    // Recovery without another staged event proves the batch was replayed, and
    // an unchanged context listing proves no full sync was needed to do it.
    _source_state.list_versions_errors.erase(a);
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(2));
    EXPECT_EQ(_source_state.list_contexts_calls, listings);
}

TEST_F(mirroring_task_test, tail_mode_config_unavailable_replays_the_batch) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    const auto listings = _source_state.list_contexts_calls;

    // A batch of only mode/config targets skips the subject phase, so the
    // source going unavailable here exercises the mode/config leg's own exit.
    _source_state.modes.emplace(a, ppsr::mode::read_only);
    _source_state.read_mode_errors.emplace(
      a,
      srs::source_error{
        .kind = srs::source_error_kind::source_unavailable,
        .message = "source down reading mode"});
    stage_tail(mode_configs_changed({a}));

    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());
    EXPECT_GT(_tail_state.rewinds, 0);

    _source_state.read_mode_errors.erase(a);
    wait_for_sync_status([this, &a](const auto&) {
        return _registry.modes().contains(a);
    }).get();
    EXPECT_EQ(_registry.modes().at(a), ppsr::mode::read_only);
    EXPECT_EQ(_source_state.list_contexts_calls, listings);
}

TEST_F(
  mirroring_task_test, tail_context_listing_unavailable_replays_the_batch) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    const auto listings = _source_state.list_versions_calls;

    // A batch of only contexts reaches the deletion phase, whose listing is the
    // third source read a tick can be refused.
    _source_state.list_contexts_error = srs::source_error{
      .kind = srs::source_error_kind::source_unavailable,
      .message = "source down listing contexts"};
    auto batch = srs::tail_batch{};
    batch.contexts.insert(ppsr::default_context);
    stage_tail(std::move(batch));

    ASSERT_TRUE(wait_for_task_state(model::task_state::link_unavailable).get());
    EXPECT_GT(_tail_state.rewinds, 0);

    _source_state.list_contexts_error.reset();
    ASSERT_TRUE(wait_for_task_state(model::task_state::active).get());
    // Replayed rather than covered by a full sync, which would have re-listed
    // every subject's versions.
    EXPECT_EQ(_source_state.list_versions_calls, listings);
}

TEST_F(mirroring_task_test, tail_sync_propagates_a_source_soft_delete) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));
    ASSERT_EQ(_registry.get_all()[0].deleted, ppsr::is_deleted::no);

    // The source soft-deletes the version it had already replicated. The tail
    // re-reads the subject and re-imports the now-deleted body, which
    // overwrites the destination version's deleted flag.
    _source_state.soft_delete(a, 1);
    stage_tail(subjects_changed({a}));

    wait_for_sync_status([this](const auto&) {
        const auto& all = _registry.get_all();
        return all.size() == 1 && all[0].deleted == ppsr::is_deleted::yes;
    }).get();
    EXPECT_EQ(_registry.get_all()[0].deleted, ppsr::is_deleted::yes);
}

TEST_F(mirroring_task_test, tail_sync_replicates_mode_and_config_targets) {
    // The mode/config leg of a tail tick, across all three target shapes a
    // CONFIG/MODE record can name: a subject, a context-level target (empty
    // subject), and the registry-wide global.
    auto a = ppsr::context_subject::unqualified("a");
    auto default_ctx = ppsr::context_subject{
      ppsr::default_context, ppsr::subject{""}};
    auto global = ppsr::global_mode_config_target;
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    const auto listings_after_full_sync = _source_state.list_versions_calls;

    // Overrides appear at the source only after the full sync, so each can
    // only have arrived on a tail tick.
    _source_state.configs.emplace(a, ppsr::compatibility_level::full);
    _source_state.modes.emplace(default_ctx, ppsr::mode::read_only);
    _source_state.configs.emplace(global, ppsr::compatibility_level::none);
    stage_tail(mode_configs_changed({a, default_ctx, global}));

    wait_for_sync_status([this, &a, &default_ctx, &global](const auto&) {
        return _registry.configs().contains(a)
               && _registry.modes().contains(default_ctx)
               && _registry.configs().contains(global);
    }).get();

    EXPECT_EQ(_registry.configs().at(a), ppsr::compatibility_level::full);
    EXPECT_EQ(_registry.modes().at(default_ctx), ppsr::mode::read_only);
    EXPECT_EQ(_registry.configs().at(global), ppsr::compatibility_level::none);
    // Mode/config work needs no version discovery, so the tick did not list a
    // single subject at the source.
    EXPECT_EQ(_source_state.list_versions_calls, listings_after_full_sync);
}

TEST_F(
  mirroring_task_test, tail_sync_ignores_out_of_scope_mode_config_targets) {
    auto in = ppsr::context_subject::unqualified("in-scope");
    auto out = ppsr::context_subject::unqualified("out-of-scope");
    _source_state.add(in, 1);
    _source_state.add(out, 1);

    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->filter.subjects.push_back("in-scope");

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_first_full_sync().get();

    _source_state.configs.emplace(in, ppsr::compatibility_level::full);
    _source_state.configs.emplace(out, ppsr::compatibility_level::full);
    stage_tail(mode_configs_changed({in, out}));

    wait_for_sync_status([this, &in](const auto&) {
        return _registry.configs().contains(in);
    }).get();
    // The reader reports every source change; the filter is the task's job.
    EXPECT_FALSE(_registry.configs().contains(out));
}

TEST_F(mirroring_task_test, tail_sync_honours_context_remapping) {
    // A tail import writes under the mapped destination context, like a full
    // sync does. Collapses source .prod onto the destination default context so
    // the test does not depend on the qualified-subjects cluster config.
    auto prod = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(prod, 1);

    auto metadata = get_default_metadata();
    auto* api = metadata.configuration.schema_registry_sync_cfg.api_mode();
    api->filter.contexts.push_back(".prod");
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", std::string{ppsr::default_context()});
    api->destination = std::move(mapping);

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    _source_state.add(prod, 2);
    stage_tail(subjects_changed({prod}));

    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    // Both versions live in the destination default context, not .prod.
    auto dest = ppsr::context_subject::unqualified("orders-value");
    EXPECT_THAT(
      _registry.get_all(),
      testing::ElementsAre(stored_for(dest), stored_for(dest)));
}

// Fixture whose destination counts inventory scans, so a test can assert
// directly how many a sync performed.
class mirroring_task_scan_count_test : public mirroring_task_test {
protected:
    schema::registry* dest() override { return &_counting; }
    scan_counting_registry _counting{&_registry};
};

TEST_F(mirroring_task_scan_count_test, empty_tail_tick_reads_neither_side) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto full = wait_for_first_full_sync().get();

    // Nothing staged, so every tick below polls an empty batch. That must cost
    // no source discovery and no destination scan -- the near-zero-cost path
    // the whole design rests on. Both are measured, not inferred from counters
    // a no-op scan would leave unchanged anyway.
    const auto listings = _source_state.list_versions_calls;
    const auto scans = _counting.scans;
    const auto polls_before = _tail_state.polls;
    ::tests::cooperative_spin_wait_with_timeout(
      wait_interval,
      [this, polls_before]() { return _tail_state.polls > polls_before + 1; })
      .get();

    EXPECT_EQ(_source_state.list_versions_calls, listings);
    EXPECT_EQ(_counting.scans, scans);
    auto status = current_sync_status();
    EXPECT_EQ(status.totals_since_task_start.errors, 0);
    EXPECT_EQ(
      status.last_full_sync->finish_time, full.last_full_sync->finish_time);
}

TEST_F(
  mirroring_task_scan_count_test, mode_config_only_tick_reads_no_subjects) {
    auto a = ppsr::context_subject::unqualified("a");
    auto global = ppsr::global_mode_config_target;
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    const auto scans = _counting.scans;
    const auto listings = _source_state.list_versions_calls;

    // A CONFIG record names its target and no subject, so the tick has nothing
    // to discover, diff or purge -- and must not pay a whole-registry
    // destination scan to find that out.
    _source_state.configs.emplace(global, ppsr::compatibility_level::none);
    stage_tail(mode_configs_changed({global}));

    wait_for_sync_status([this, &global](const auto&) {
        return _registry.configs().contains(global);
    }).get();

    EXPECT_EQ(_registry.configs().at(global), ppsr::compatibility_level::none);
    EXPECT_EQ(_counting.scans, scans);
    EXPECT_EQ(_source_state.list_versions_calls, listings);
    auto status = current_sync_status();
    EXPECT_GT(status.totals_since_task_start.compatibility_configs_changed, 0);
    EXPECT_EQ(status.totals_since_task_start.errors, 0);
}

TEST_F(
  mirroring_task_scan_count_test,
  tail_import_rescans_and_republishes_inventory) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    auto full = wait_for_first_full_sync().get();
    ASSERT_EQ(full.inventory.destination_subject_versions, 1);
    const auto scans_after_full_sync = _counting.scans;

    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));

    // The reported destination inventory must catch up with what the tail
    // imported, as it does after a full sync, rather than describing the
    // pre-import baseline the diff was taken against.
    auto status = wait_for_sync_status([](const auto& s) {
                      return s.inventory.destination_subject_versions == 2;
                  }).get();
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->inventory.destination_subject_versions, 2);
    // Twice: once to seed the diff, once to re-read the result.
    EXPECT_EQ(_counting.scans, scans_after_full_sync + 2);
}

TEST_F(mirroring_task_test, tail_sync_faults_on_an_unmapped_context) {
    // A context created after the last full sync is named by the tail before
    // any full sync can discover it, so the tail vets the batch itself.
    // Otherwise the import degrades to per-item errors here while the same
    // configuration faults the task on the full path.
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    auto metadata = get_default_metadata();
    auto* api = metadata.configuration.schema_registry_sync_cfg.api_mode();
    // Covers the only context the source has, so the full sync passes; the
    // batch below names one the mapping does not cover.
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(
      std::string{ppsr::default_context()},
      std::string{ppsr::default_context()});
    api->destination = std::move(mapping);

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    stage_sticky_tail(subjects_changed({ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}}}));

    ASSERT_TRUE(wait_for_task_state(model::task_state::faulted).get());
    auto status = current_sync_status();
    EXPECT_THAT(status.last_error_message, testing::HasSubstr(".prod"));
    // Faulted before any source read, so nothing was imported or counted.
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(1));
    EXPECT_EQ(status.totals_since_task_start.errors, 0);
}

TEST_F(
  mirroring_task_test,
  mode_config_only_tail_sync_faults_on_an_unmapped_context) {
    // The mode/config-only path returns before the subject work, and its writes
    // forward-map their target too, so the check has to sit above it.
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    auto metadata = get_default_metadata();
    auto* api = metadata.configuration.schema_registry_sync_cfg.api_mode();
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(
      std::string{ppsr::default_context()},
      std::string{ppsr::default_context()});
    api->destination = std::move(mapping);

    lead_schema_registry();
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_first_full_sync().get();

    stage_sticky_tail(mode_configs_changed(
      {ppsr::context_subject{ppsr::context{".prod"}, ppsr::subject{""}}}));

    ASSERT_TRUE(wait_for_task_state(model::task_state::faulted).get());
    EXPECT_THAT(
      current_sync_status().last_error_message, testing::HasSubstr(".prod"));
    EXPECT_THAT(_registry.modes(), testing::IsEmpty());
    EXPECT_THAT(_registry.configs(), testing::IsEmpty());
}

TEST_F(
  mirroring_task_test, tail_sync_faults_on_a_non_default_context_when_flat) {
    // The severe case. With qualified subjects disabled the destination store
    // can only hold the default context: an import would be accepted here and
    // then reparse as a literal ":.prod:orders-value" subject on the
    // destination's next store replay.
    scoped_config cfg;
    cfg.get("schema_registry_enable_qualified_subjects").set_value(false);

    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(1));

    stage_sticky_tail(subjects_changed({ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}}}));

    ASSERT_TRUE(wait_for_task_state(model::task_state::faulted).get());
    EXPECT_THAT(
      current_sync_status().last_error_message,
      testing::HasSubstr("schema_registry_enable_qualified_subjects"));
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(1));
}

TEST_F(mirroring_task_test, tail_sync_deletes_a_source_absent_context) {
    // A source-side context delete is not a change to any subject, so only the
    // CONTEXT record makes it observable to the tail. The source refuses to
    // delete a context that still has subjects, so its subject deletes were
    // recorded first and this batch carries both.
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    auto keep = ppsr::context_subject::unqualified("keep-value");
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(prod_orders, 1);
    _source_state.add(keep, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.get_all(), testing::SizeIs(2));

    // The source drops the subject and then the context itself.
    _source_state.remove_subject(prod_orders);
    _source_state.contexts.pop_back();
    auto batch = subjects_changed({prod_orders});
    batch.contexts.insert(ppsr::context{".prod"});
    stage_tail(std::move(batch));

    wait_for_sync_status([this](const auto&) {
        return _registry.list_contexts().get().size() == 1;
    }).get();
    // .prod is tombstoned and its subject purged; the untouched default-context
    // subject survives.
    EXPECT_THAT(
      _registry.list_contexts().get(),
      testing::ElementsAre(ppsr::default_context));
    EXPECT_THAT(
      _registry.get_all(),
      testing::ElementsAre(
        testing::Field(
          &ppsr::stored_schema::schema,
          testing::Property(&ppsr::subject_schema::sub, keep))));
}

TEST_F(mirroring_task_test, tail_sync_lists_contexts_only_when_one_changed) {
    // The context phase costs a source listing, so it must be paid only by a
    // batch that actually reported a context -- otherwise every ordinary tick
    // re-reads the source's contexts.
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    const auto listings = _source_state.list_contexts_calls;
    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));
    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    EXPECT_EQ(_source_state.list_contexts_calls, listings);

    // The same tick shape plus a CONTEXT record does list them.
    auto batch = subjects_changed({a});
    batch.contexts.insert(ppsr::context{".prod"});
    stage_tail(std::move(batch));
    ::tests::cooperative_spin_wait_with_timeout(
      wait_interval,
      [this, listings] { return _source_state.list_contexts_calls > listings; })
      .get();
}

TEST_F(
  mirroring_task_test, tail_sync_skips_context_deletion_when_listing_fails) {
    // The deletion set is the source's whole context list, so a failed listing
    // must delete nothing: treating it as an empty source would tombstone every
    // context the link owns.
    auto prod_orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders-value"}};
    _source_state.contexts.push_back(ppsr::context{".prod"});
    _source_state.add(prod_orders, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();
    ASSERT_THAT(_registry.list_contexts().get(), testing::SizeIs(2));

    _source_state.list_contexts_error = srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "context listing failed"};
    auto batch = srs::tail_batch{};
    batch.contexts.insert(ppsr::context{".prod"});
    stage_tail(std::move(batch));

    auto errored = wait_for_sync_status([](const auto& s) {
                       return s.totals_since_task_start.errors == 1;
                   }).get();
    ASSERT_TRUE(errored.has_value());
    EXPECT_THAT(
      _registry.list_contexts().get(),
      testing::UnorderedElementsAre(
        ppsr::default_context, ppsr::context{".prod"}));
    // Not put back: replaying it would hit the same error every tick.
    EXPECT_EQ(_tail_state.rewinds, 0);
}

TEST_F(mirroring_task_test, tail_sync_reports_itself_as_a_tail_sync) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    // Park the next tail tick inside poll() so the in-progress sync is
    // observable, then confirm the status names it a TAIL sync -- the
    // acceptance criterion that tail work surfaces in link status.
    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));
    _tail_state.open_poll_gate();

    auto in_flight = wait_for_sync_status([](const auto& s) {
                         return s.current_sync.has_value();
                     }).get();
    ASSERT_TRUE(in_flight.has_value());
    EXPECT_EQ(
      in_flight->current_sync->sync_type,
      model::schema_registry_sync_type::tail);

    // Release it and confirm the parked tick was real work, not just a status
    // blip.
    _tail_state.release_poll_gate();
    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(2));
}

TEST_F(mirroring_task_test, arm_failure_does_not_stop_the_full_sync) {
    auto a = ppsr::context_subject::unqualified("a");
    _source_state.add(a, 1);
    // A reader that throws from arm() breaks the tail_reader contract. The full
    // sync is not optional, so it must still run: without a guard the throw
    // reaches the task runner, which faults the task before any source read.
    _tail_state.arm_throws = true;

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();

    auto full = wait_for_first_full_sync().get();
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(1));
    EXPECT_TRUE(wait_for_task_state(model::task_state::active).get());
    EXPECT_GT(_tail_state.arms, 0);

    // Arming is retried by the next full sync, so a reader that recovers gets
    // tailing back on its own. A config change is what forces that sync; the
    // rate limit is inert here because the fake source reader ignores the API
    // config, so only the fact that it changed matters.
    _tail_state.arm_throws = false;
    constexpr auto changed_rate_limit
      = model::schema_registry_sync_config::shadow_schema_registry_api::
          default_max_source_requests_per_second
        - 1;
    auto metadata = get_default_metadata();
    metadata.configuration.schema_registry_sync_cfg.api_mode()
      ->max_source_requests_per_second = changed_rate_limit;
    fixture()->upsert_link(std::move(metadata)).get();
    wait_for_sync_status([&full](const auto& s) {
        return s.last_full_sync.has_value()
               && s.last_full_sync->start_time
                    != full.last_full_sync->start_time
               && !s.current_sync.has_value();
    }).get();

    // Tailing is live again: a staged batch now lands without a full sync.
    _source_state.add(a, 2);
    stage_tail(subjects_changed({a}));
    wait_for_sync_status([this](const auto&) {
        return _registry.get_all().size() == 2;
    }).get();
    EXPECT_THAT(_registry.get_all(), testing::SizeIs(2));
}

TEST_F(mirroring_task_test, tail_reader_is_armed_once_per_full_sync) {
    _source_state.add(ppsr::context_subject::unqualified("a"), 1);

    lead_schema_registry();
    fixture()->upsert_link(get_default_metadata()).get();
    wait_for_first_full_sync().get();

    // Armed by the full sync, before its first source read; the tail ticks that
    // follow poll without re-arming.
    EXPECT_EQ(_tail_state.arms, 1);
    const auto polls_before = _tail_state.polls;
    ::tests::cooperative_spin_wait_with_timeout(
      wait_interval,
      [this, polls_before]() { return _tail_state.polls > polls_before; })
      .get();
    EXPECT_EQ(_tail_state.arms, 1);
}

} // namespace cluster_link::tests
