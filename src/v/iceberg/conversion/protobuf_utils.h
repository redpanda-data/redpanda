/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "google/protobuf/descriptor.h"

#include <deque>

namespace iceberg {

using proto_descriptors_stack = std::deque<const google::protobuf::Descriptor*>;
inline constexpr int max_recursion_depth = 100;

bool is_recursive_type(
  const google::protobuf::Descriptor& msg,
  const proto_descriptors_stack& stack);

namespace protobuf {

/// Redpanda datalake well-known protobuf types namespace.
constexpr std::string_view datalake_well_known_type_prefix
  = "redpanda.datalake.";

constexpr std::string_view datalake_date_type = "redpanda.datalake.Date";
} // namespace protobuf

} // namespace iceberg
