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

bool is_valid(
  const ss::sstring key,
  const cluster::topic_configuration& topic_cfg,
  const std::unordered_map<
    config::constraint_t::key_type,
    config::constraint_t>& constraints) {
    return config::topic_config_satisfies_constraint(
      topic_cfg, constraints.at(key));
}

TEST(ConfigConstraintsTest, ConstraintValidation) {
    auto cfg = test_config{};
    const auto& constraints = cfg.restrict_constraints();
    cluster::topic_configuration topic_cfg;

    auto name = ss::sstring{"log_cleanup_policy"};
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    EXPECT_TRUE(is_valid(name, topic_cfg, constraints));
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    EXPECT_FALSE(is_valid(name, topic_cfg, constraints));

    name = ss::sstring{"default_topic_replications"};
    topic_cfg.replication_factor = LIMIT_MIN;
    EXPECT_TRUE(is_valid(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MAX;
    EXPECT_TRUE(is_valid(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    EXPECT_FALSE(is_valid(name, topic_cfg, constraints));
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    EXPECT_FALSE(is_valid(name, topic_cfg, constraints));

    name = ss::sstring{"log_retention_ms"};
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS);
    EXPECT_TRUE(is_valid(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS);
    EXPECT_TRUE(is_valid(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
    EXPECT_FALSE(is_valid(name, topic_cfg, constraints));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
    EXPECT_FALSE(is_valid(name, topic_cfg, constraints));
}

void do_clamp(
  const ss::sstring key,
  cluster::topic_configuration& topic_cfg,
  const std::unordered_map<
    config::constraint_t::key_type,
    config::constraint_t>& constraints) {
    auto clamp = [](
                   cluster::topic_configuration& topic_cfg,
                   const config::constraint_t& constraint) {
        config::constraint_clamp_topic_config(topic_cfg, constraint);
    };
    return clamp(topic_cfg, constraints.at(key));
}

TEST(ConfigConstraintsTest, ConstraintClamping) {
    auto cfg = test_config{};
    const auto& constraints = cfg.clamp_constraints();
    cluster::topic_configuration topic_cfg;

    // Check enum clamp constraint
    auto name = ss::sstring{"log_cleanup_policy"};
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.log_cleanup_policy());

    // Check integral clamp constraint
    name = ss::sstring{"default_topic_replications"};
    topic_cfg.replication_factor = LIMIT_MIN;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MAX);
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MAX);

    // Check ms clamp constraint
    name = ss::sstring{"log_retention_ms"};
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS);
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS);
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
    do_clamp(name, topic_cfg, constraints);
    EXPECT_EQ(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS));
}

TEST(ConfigConstraintsTest, ConstraintApply) {
    auto cfg = test_config{};
    auto name = ss::sstring{"default_topic_replications"};
    auto& restrict_constraint = cfg.restrict_constraints().at(name);
    auto& clamp_constraint = cfg.clamp_constraints().at(name);

    // Test valid config
    {
        cluster::topic_configuration topic_cfg;
        topic_cfg.replication_factor = LIMIT_MIN;
        std::vector<kafka::creatable_topic_result> err_vec;

        // Restrict constraint - expect no error
        apply_constraint(
          topic_cfg,
          restrict_constraint,
          kafka::creatable_topic_result{},
          std::back_inserter(err_vec));
        EXPECT_TRUE(err_vec.empty());

        // Clamp constraint - expect that the RF is unchanged
        auto copy_rf = topic_cfg.replication_factor;
        apply_constraint(
          topic_cfg,
          clamp_constraint,
          kafka::creatable_topic_result{},
          std::back_inserter(err_vec));
        EXPECT_EQ(topic_cfg.replication_factor, copy_rf);
    }

    // Test invalid config
    {
        cluster::topic_configuration topic_cfg;
        topic_cfg.replication_factor = LIMIT_MIN - 1;
        std::vector<kafka::creatable_topic_result> err_vec;
        kafka::creatable_topic_result err_res{
          .name = model::topic{"test_topic"}};

        // Restrict constraint - expect error
        apply_constraint(
          topic_cfg, restrict_constraint, err_res, std::back_inserter(err_vec));
        EXPECT_EQ(err_vec.size(), 1);
        EXPECT_EQ(err_vec[0].error_code, kafka::error_code::invalid_config);
        EXPECT_TRUE(err_vec[0].error_message.has_value());
        EXPECT_EQ(
          *err_vec[0].error_message,
          ss::sstring{
            "Configuration breaks constraint default_topic_replications"});

        // Clamp constraint - expect that the RF is the minimum
        apply_constraint(
          topic_cfg, clamp_constraint, err_res, std::back_inserter(err_vec));
        EXPECT_EQ(topic_cfg.replication_factor, LIMIT_MIN);
    }
}

} // namespace
