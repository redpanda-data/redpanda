// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "config/bounded_property.h"
#include "config/configuration.h"
#include "config/property.h"
#include "config/tests/constraint_utils.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/util/log.hh>

namespace {
struct test_config : public config::config_store {
    config::bounded_property<int16_t> default_topic_replication;
    config::property<model::cleanup_policy_bitflags> log_cleanup_policy;
    config::retention_duration_property log_retention_ms;
    config::one_or_many_map_property<config::constraint_t> restrict_constraints;
    config::one_or_many_map_property<config::constraint_t> clamp_constraints;

    test_config()
      : default_topic_replication(
        *this, "default_topic_replications", "An integral property", {}, 1, {})
      , log_cleanup_policy(
          *this,
          "log_cleanup_policy",
          "An enum property",
          {},
          model::cleanup_policy_bitflags::deletion)
      , log_retention_ms(*this, "log_retention_ms", "A ms property", {}, 1h)
      , restrict_constraints(
          *this,
          "restrict_constraints",
          "A sequence of constraints for testing configs with restrict "
          "constraint "
          "type",
          {},
          make_constraints(config::constraint_type::restrikt))
      , clamp_constraints(
          *this,
          "clamp_constraints",
          "A sequence of constraints for testing configs with clamp constraint "
          "type",
          {},
          make_constraints(config::constraint_type::clamp)) {}
};

bool do_apply(
  const config::constraint_t::key_type key,
  cluster::topic_configuration& topic_cfg,
  const std::unordered_map<
    config::constraint_t::key_type,
    config::constraint_t>& constraints) {
    return config::apply_constraint(topic_cfg, constraints.at(key));
}

TEST(ConfigConstraintsTest, ConstraintValidation) {
    auto cfg = test_config{};
    const auto& constraints = cfg.restrict_constraints();
    cluster::topic_configuration topic_cfg;

    auto name = ss::sstring{"log_cleanup_policy"};
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    EXPECT_TRUE(do_apply(name, topic_cfg, constraints));
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));

    name = ss::sstring{"default_topic_replications"};
    topic_cfg.replication_factor = LIMIT_MIN;
    EXPECT_TRUE(do_apply(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MAX;
    EXPECT_TRUE(do_apply(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));

    name = ss::sstring{"log_retention_ms"};
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MIN_MS};
    EXPECT_TRUE(do_apply(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MAX_MS};
    EXPECT_TRUE(do_apply(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MIN_MS - 1ms};
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MAX_MS + 1ms};
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));
    // Disabled state (infinite retention) with a [min,max] range is not valid
    // because infinity > max
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{};
    EXPECT_FALSE(do_apply(name, topic_cfg, constraints));
}

TEST(ConfigConstraintsTest, ConstraintClamping) {
    auto cfg = test_config{};
    const auto& constraints = cfg.clamp_constraints();
    cluster::topic_configuration topic_cfg;

    // Check enum clamp constraint
    auto name = ss::sstring{"log_cleanup_policy"};
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());

    // Check integral clamp constraint
    name = ss::sstring{"default_topic_replications"};
    topic_cfg.replication_factor = LIMIT_MIN;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MAX);
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MAX);

    // Check ms clamp constraint
    name = ss::sstring{"log_retention_ms"};
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MIN_MS};
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>{LIMIT_MIN_MS});
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MAX_MS};
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>{LIMIT_MAX_MS});
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MIN_MS - 1ms};
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>{LIMIT_MIN_MS});
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{LIMIT_MAX_MS + 1ms};
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>{LIMIT_MAX_MS});
    // Disabled state (infinite retention) with a [min,max] range, value should
    // become max because infinity > max
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{};
    do_apply(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>{LIMIT_MAX_MS});
}
} // namespace
