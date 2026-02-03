// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <array>
#include <iostream>

namespace cluster::client_quota {

/// client_quota::rule is used for reporting metrics to show which type of rule
/// is being used for limiting clients
enum class rule {
    not_applicable,
    kafka_client_default,
    kafka_client_prefix,
    kafka_client_id,
    kafka_user_default,
    kafka_user_default_client_default,
    kafka_user_default_client_prefix,
    kafka_user_default_client_id,
    kafka_user,
    kafka_user_client_default,
    kafka_user_client_prefix,
    kafka_user_client_id
};

inline constexpr std::array all_client_quota_rules = {
  rule::not_applicable,
  rule::kafka_client_default,
  rule::kafka_client_prefix,
  rule::kafka_client_id,
  rule::kafka_user_default,
  rule::kafka_user_default_client_default,
  rule::kafka_user_default_client_prefix,
  rule::kafka_user_default_client_id,
  rule::kafka_user,
  rule::kafka_user_client_default,
  rule::kafka_user_client_prefix,
  rule::kafka_user_client_id};

std::ostream& operator<<(std::ostream&, rule);

} // namespace cluster::client_quota
