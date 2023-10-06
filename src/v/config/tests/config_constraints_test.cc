// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/tests/constraints_utils.h"

#include <seastar/testing/thread_test_case.hh>

namespace {

SEASTAR_THREAD_TEST_CASE(test_constraint_validation) {
    auto cfg = test_config();

    cluster::topic_configuration topic_cfg;

    // Check restrict constraints
    {
        auto& enum_constraint = cfg.restrict_constraints()[0];
        topic_cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        BOOST_CHECK(enum_constraint.validate(topic_cfg));
        topic_cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        BOOST_CHECK(!enum_constraint.validate(topic_cfg));

        auto& integral_constraint = cfg.restrict_constraints()[1];
        topic_cfg.replication_factor = LIMIT_MIN + 1;
        BOOST_CHECK(integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MAX - 1;
        BOOST_CHECK(integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MIN - 1;
        BOOST_CHECK(!integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MAX + 1;
        BOOST_CHECK(!integral_constraint.validate(topic_cfg));

        auto& ms_constraint = cfg.restrict_constraints()[2];
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms);
        BOOST_CHECK(ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms);
        BOOST_CHECK(ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
        BOOST_CHECK(!ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
        BOOST_CHECK(!ms_constraint.validate(topic_cfg));
    }

    // Check clamp constraints
    {
        auto& enum_constraint = cfg.clamp_constraints()[0];
        topic_cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        BOOST_CHECK(enum_constraint.validate(topic_cfg));
        topic_cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        BOOST_CHECK(!enum_constraint.validate(topic_cfg));

        auto& integral_constraint = cfg.clamp_constraints()[1];
        topic_cfg.replication_factor = LIMIT_MIN + 1;
        BOOST_CHECK(integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MAX - 1;
        BOOST_CHECK(integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MIN - 1;
        BOOST_CHECK(!integral_constraint.validate(topic_cfg));
        topic_cfg.replication_factor = LIMIT_MAX + 1;
        BOOST_CHECK(!integral_constraint.validate(topic_cfg));

        auto& ms_constraint = cfg.clamp_constraints()[2];
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms);
        BOOST_CHECK(ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms);
        BOOST_CHECK(ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
        BOOST_CHECK(!ms_constraint.validate(topic_cfg));
        topic_cfg.properties.retention_duration
          = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
        BOOST_CHECK(!ms_constraint.validate(topic_cfg));
    }
}

SEASTAR_THREAD_TEST_CASE(test_constraint_clamping) {
    auto cfg = test_config();

    cluster::topic_configuration topic_cfg;

    // Check enum clamp constraint
    auto& enum_constraint = cfg.clamp_constraints()[0];
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    enum_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.original_enum());
    topic_cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    enum_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.cleanup_policy_bitflags, cfg.original_enum());

    // Check integral clamp constraint
    auto& integral_constraint = cfg.clamp_constraints()[1];
    topic_cfg.replication_factor = LIMIT_MIN + 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MIN + 1);
    topic_cfg.replication_factor = LIMIT_MAX - 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MAX - 1);
    topic_cfg.replication_factor = LIMIT_MIN - 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MIN);
    topic_cfg.replication_factor = LIMIT_MAX + 1;
    integral_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(topic_cfg.replication_factor, LIMIT_MAX);

    // Check ms clamp constraint
    auto& ms_constraint = cfg.clamp_constraints()[2];
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS + 1ms));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS - 1ms));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MIN_MS - 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MIN_MS));
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(LIMIT_MAX_MS + 1ms);
    ms_constraint.clamp(topic_cfg);
    BOOST_CHECK_EQUAL(
      topic_cfg.properties.retention_duration,
      tristate<std::chrono::milliseconds>(LIMIT_MAX_MS));
}

} // namespace
