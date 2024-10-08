// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg::tests {

enum class value_pattern {
    zeros,
    random,
};
struct value_spec {
    value_pattern pattern{value_pattern::zeros};

    // Percentage of time (out of 100) optional fields will have null values
    // generated for them.
    int null_pct = 0;

    // Number of elements to include in list and map fields.
    size_t max_elements = 5;

    // Max size of strings and bytes.
    size_t max_strlen = 5;

    // If set, numeric primitives will have values based on this value.
    std::optional<int64_t> forced_num_val = std::nullopt;

    std::optional<iobuf> forced_fixed_val = std::nullopt;
};

// Creates an Iceberg value in accordance to the provided specification.
value make_value(const value_spec&, const field_type&);
struct_value make_struct_value(const value_spec&, const field_type&);

} // namespace iceberg::tests
