// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "google/protobuf/descriptor.h"

#include <deque>

namespace datalake {

using proto_descriptors_stack = std::deque<const google::protobuf::Descriptor*>;
inline constexpr int max_recursion_depth = 100;

bool is_recursive_type(
  const google::protobuf::Descriptor& msg,
  const proto_descriptors_stack& stack);

} // namespace datalake
