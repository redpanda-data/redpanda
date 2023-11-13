// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/constraints.h"

#include <chrono>
#include <optional>
#include <unordered_map>

using namespace std::chrono_literals;

namespace {

constexpr static int16_t LIMIT_MIN = 3;
constexpr static int16_t LIMIT_MAX = 10;
constexpr static std::chrono::milliseconds LIMIT_MIN_MS = 1s;
constexpr static std::chrono::milliseconds LIMIT_MAX_MS = 5s;

std::unordered_map<config::constraint_t::key_type, config::constraint_t>
make_constraints(config::constraint_type type) {
    auto integral_constraint_t = config::constraint_t{
      .name = "default_topic_replications",
      .type = type,
      .flags = config::range_values<int64_t>(LIMIT_MIN, LIMIT_MAX)};
    auto enum_constraint_t = config::constraint_t{
      .name = "log_cleanup_policy",
      .type = type,
      .flags = config::constraint_enabled_t::yes};
    auto ms_constraint_t = config::constraint_t{
      .name = "log_retention_ms",
      .type = type,
      .flags = config::range_values<int64_t>(
        LIMIT_MIN_MS.count(), LIMIT_MAX_MS.count())};

    std::unordered_map<config::constraint_t::key_type, config::constraint_t>
      res;
    res.reserve(3);
    res[integral_constraint_t.name] = std::move(integral_constraint_t);
    res[enum_constraint_t.name] = std::move(enum_constraint_t);
    res[ms_constraint_t.name] = std::move(ms_constraint_t);
    return res;
}

} // namespace
