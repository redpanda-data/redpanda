// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "config/config_store.h"
#include "config/property.h"
#include "config/property_constraints.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/testing/thread_test_case.hh>

#include <chrono>
#include <optional>

using namespace std::chrono_literals;

namespace {

constexpr static int16_t LIMIT_MIN = 3;
constexpr static int16_t LIMIT_MAX = 10;
constexpr static std::chrono::milliseconds LIMIT_MIN_MS = 1s;
constexpr static std::chrono::milliseconds LIMIT_MAX_MS = 5s;

std::vector<config::constraint_t> make_constraints(
  config::constraint_type type,
  config::binding<model::cleanup_policy_bitflags> enum_binding) {
    std::vector<config::constraint_t> res;
    res.reserve(3);

    res.emplace_back(
      "log_cleanup_policy",
      type,
      ss::make_shared<
        config::constraint_validator_enabled<model::cleanup_policy_bitflags>>(
        config::constraint_enabled_t::yes,
        enum_binding,
        [](const cluster::topic_configuration& topic_cfg) {
            return topic_cfg.properties.cleanup_policy_bitflags
                     ? *topic_cfg.properties.cleanup_policy_bitflags
                     : model::cleanup_policy_bitflags::none;
        },
        [](
          cluster::topic_configuration& topic_cfg,
          const model::cleanup_policy_bitflags& topic_val) {
            topic_cfg.properties.cleanup_policy_bitflags = topic_val;
        }));

    res.emplace_back(
      "default_topic_replications",
      type,
      ss::make_shared<config::constraint_validator_range<int16_t>>(
        LIMIT_MIN,
        LIMIT_MAX,
        [](const cluster::topic_configuration& topic_cfg) {
            return topic_cfg.replication_factor;
        },
        [](cluster::topic_configuration& topic_cfg, const int16_t& topic_val) {
            topic_cfg.replication_factor = topic_val;
        }));

    res.emplace_back(
      "log_retention_ms",
      type,
      ss::make_shared<
        config::constraint_validator_range<std::chrono::milliseconds>>(
        LIMIT_MIN_MS,
        LIMIT_MAX_MS,
        [](const cluster::topic_configuration& topic_cfg) {
            if (topic_cfg.properties.retention_duration.has_optional_value()) {
                return topic_cfg.properties.retention_duration.value();
            } else {
                return LIMIT_MIN_MS;
            }
        },
        [](
          cluster::topic_configuration& topic_cfg,
          const std::chrono::milliseconds& topic_val) {
            topic_cfg.properties.retention_duration
              = tristate<std::chrono::milliseconds>(topic_val);
        }));

    return res;
}

struct test_config : public config::config_store {
    config::property<model::cleanup_policy_bitflags> original_enum;
    config::one_or_many_property<config::constraint_t> restrict_constraints;
    config::one_or_many_property<config::constraint_t> clamp_constraints;

    test_config()
      : original_enum(
        *this,
        "log_cleanup_policy",
        "An enumertaion property",
        {.needs_restart = config::needs_restart::no},
        model::cleanup_policy_bitflags::deletion)
      , restrict_constraints(
          *this,
          "restrict_constraints",
          "A sequence of constraints for testing configs with restrict "
          "constraint "
          "type",
          {},
          make_constraints(
            config::constraint_type::restrikt, original_enum.bind()))
      , clamp_constraints(
          *this,
          "clamp_constraints",
          "A sequence of constraints for testing configs with clamp constraint "
          "type",
          {},
          make_constraints(
            config::constraint_type::clamp, original_enum.bind())) {}
};

} // namespace
