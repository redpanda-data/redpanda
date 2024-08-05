/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <string_view>

namespace kafka {

/**
 * Topic property names.
 */
inline constexpr std::string_view topic_property_retention_bytes
  = "retention.bytes";

inline constexpr std::string_view topic_property_retention_duration
  = "retention.ms";

inline constexpr std::string_view topic_property_cleanup_policy
  = "cleanup.policy";

inline constexpr std::string_view topic_property_compression
  = "compression.type";

inline constexpr std::string_view topic_property_retention_local_target_bytes
  = "retention.local.target.bytes";

inline constexpr std::string_view topic_property_retention_local_target_ms
  = "retention.local.target.ms";

inline constexpr std::string_view
  topic_property_initial_retention_local_target_bytes
  = "initial.retention.local.target.bytes";

inline constexpr std::string_view
  topic_property_initial_retention_local_target_ms
  = "initial.retention.local.target.ms";

} // namespace kafka
