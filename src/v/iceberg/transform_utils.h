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

#include "base/outcome.h"
#include "iceberg/transform.h"
#include "iceberg/values.h"

namespace iceberg {
class partition_spec_field_error : public std::exception {
public:
    explicit partition_spec_field_error(std::string msg) noexcept
      : msg_(std::move(msg)) {}

    const char* what() const noexcept final { return msg_.c_str(); }

private:
    std::string msg_;
};
// Transforms the given value to its appropriate Iceberg value based on the
// input transform.
//
// This will throw if used for anything else!
std::optional<value> apply_transform(const value&, const transform&);

// Returns true if the given transform can be applied to the given primitive
checked<std::nullopt_t, partition_spec_field_error>
validate_transform_can_be_applied(const transform&, const field_type&);
} // namespace iceberg
