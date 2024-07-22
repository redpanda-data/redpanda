// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/datatypes.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

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
  hour_transform,
  void_transform>;
bool operator==(const transform& lhs, const transform& rhs);

struct partition_field {
    using id_t = named_type<int32_t, struct field_id_tag>;
    nested_field::id_t source_id;
    id_t field_id;
    ss::sstring name;
    transform transform;

    friend bool operator==(const partition_field&, const partition_field&)
      = default;
};

struct partition_spec {
    using id_t = named_type<int32_t, struct spec_id_tag>;
    id_t spec_id;
    chunked_vector<partition_field> fields;

    friend bool operator==(const partition_spec&, const partition_spec&)
      = default;
};

} // namespace iceberg
