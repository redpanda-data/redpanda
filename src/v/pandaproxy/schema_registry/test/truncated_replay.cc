// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "kafka/data/partition_proxy.h"
#include "model/ktp.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/types.h"
#include "redpanda/tests/fixture.h"
#include "schema/registry.h"

#include <gtest/gtest.h>

namespace pps = pandaproxy::schema_registry;

namespace {

const model::ktp schemas_ktp{
  model::schema_registry_internal_tp.topic,
  model::schema_registry_internal_tp.partition};

pps::subject_schema make_schema(pps::context_subject sub) {
    return pps::subject_schema{
      std::move(sub),
      pps::schema_definition{
        R"({"type":"record","name":"r","fields":[{"name":"f","type":"string"}]})",
        pps::schema_type::avro}};
}

} // namespace

/// Covers what happens when the `_schemas` topic has been prefix truncated.
/// Replay then starts at the earliest surviving offset instead of 0, keeping
/// the registry available, at the cost of whatever the deleted records held.
class truncated_replay_fixture
  : public redpanda_thread_fixture
  , public ::testing::TestWithParam<bool> {
public:
    truncated_replay_fixture() {
        wait_for_controller_leadership().get();
        make_registry();
    }

    void make_registry() {
        registry = schema::registry::make_default(app.schema_registry().get());
    }

    /// Highest kafka offset written to `_schemas` so far, exclusive.
    model::offset kafka_high_watermark() {
        return on_schemas_partition(
          [](kafka::partition_proxy& p) { return p.high_watermark(); });
    }

    /// Earliest kafka offset still available on `_schemas`.
    model::offset kafka_start_offset() {
        return on_schemas_partition(
          [](kafka::partition_proxy& p) { return p.start_offset(); });
    }

    /// Restart the broker with `_schemas` no longer exempt from log eviction,
    /// and with the schema store loaded lazily so that a test can truncate
    /// before the first replay.
    void restart_with_eviction_enabled(bool use_rpc) {
        ss::smp::invoke_on_all([use_rpc] {
            auto& cfg = config::shard_local_cfg();
            cfg.log_eviction_exempt_topics.set_value(
              std::vector<ss::sstring>{});
            cfg.schema_registry_replay_on_startup.set_value(false);
            cfg.schema_registry_use_rpc.set_value(use_rpc);
        }).get();
        restart(should_wipe::no);
        wait_for_controller_leadership().get();
        wait_for_leader(schemas_ktp.to_ntp()).get();
        make_registry();
    }

    /// What an operator who enables deletion on `_schemas` and then calls
    /// DeleteRecords ends up with: the topic becomes collectable, and the log
    /// is prefix truncated.
    void enable_deletion_and_prefix_truncate(model::offset kafka_offset) {
        auto shard = app.shard_table.local().shard_for(schemas_ktp.to_ntp());
        ASSERT_TRUE(shard.has_value());
        auto ec = app.partition_manager
                    .invoke_on(
                      *shard,
                      [kafka_offset](this auto, cluster::partition_manager& mgr)
                        -> ss::future<std::error_code> {
                          auto partition = mgr.get(schemas_ktp.to_ntp());
                          auto props
                            = partition->get_topic_config()->get().properties;
                          props.cleanup_policy_bitflags
                            = model::cleanup_policy_bitflags::compaction
                              | model::cleanup_policy_bitflags::deletion;
                          co_await partition->update_configuration(
                            std::move(props));
                          co_return co_await partition->prefix_truncate(
                            partition->log()->to_log_offset(kafka_offset),
                            model::offset_cast(kafka_offset),
                            ss::lowres_clock::time_point::max());
                      })
                    .get();
        ASSERT_FALSE(ec) << ec.message();
    }

    std::unique_ptr<schema::registry> registry;

private:
    template<typename Fn>
    model::offset on_schemas_partition(Fn fn) {
        auto shard = app.shard_table.local().shard_for(schemas_ktp.to_ntp());
        vassert(shard.has_value(), "_schemas partition not found");
        return app.partition_manager
          .invoke_on(
            *shard,
            [fn](cluster::partition_manager& mgr) {
                auto proxy = kafka::make_partition_proxy(schemas_ktp, mgr);
                vassert(proxy.has_value(), "_schemas partition proxy");
                return fn(*proxy);
            })
          .get();
    }
};

TEST_P(truncated_replay_fixture, replays_from_the_earliest_surviving_offset) {
    auto lost = pps::context_subject::unqualified("lost-subject");
    auto kept = pps::context_subject::unqualified("kept-subject");

    registry->create_schema(make_schema(lost)).get();
    // Everything written for `lost` is below this offset, and everything
    // written for `kept` is at or above it.
    auto truncate_at = kafka_high_watermark();
    registry->create_schema(make_schema(kept)).get();
    ASSERT_GT(truncate_at, model::offset{0});

    restart_with_eviction_enabled(GetParam());
    enable_deletion_and_prefix_truncate(truncate_at);
    ASSERT_EQ(kafka_start_offset(), truncate_at);

    // This is the first read since the restart, so it drives the replay. A
    // replay from offset 0 would fail with offset_out_of_range: the registry
    // must come up and serve what survived.
    auto stored = registry->get_subject_schema(kept, std::nullopt).get();
    EXPECT_EQ(stored.schema.sub(), kept);

    // The truncated record is unrecoverable, so its subject is gone.
    EXPECT_THROW(
      registry->get_subject_schema(lost, std::nullopt).get(), pps::exception);
}

INSTANTIATE_TEST_SUITE_P(
  Transport,
  truncated_replay_fixture,
  ::testing::Values(true, false),
  [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "rpc" : "kafka_client";
  });
