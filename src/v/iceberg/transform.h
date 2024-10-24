// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include <cstdint>
#include <variant>

namespace iceberg {

struct identity_transform {};
struct bucket_transform {
    uint32_t n;
};
struct truncate_transform {
    uint32_t length;
};
struct year_transform {};
struct month_transform {};
struct day_transform {};
struct hour_transform {};
struct void_transform {};

using transform = std::variant<
  identity_transform,
  bucket_transform,
  truncate_transform,
  year_transform,
  month_transform,
  day_transform,
  hour_transform,
  void_transform>;
bool operator==(const transform& lhs, const transform& rhs);

} // namespace iceberg
