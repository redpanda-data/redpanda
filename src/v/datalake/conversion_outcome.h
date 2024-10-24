// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "iceberg/values.h"

namespace datalake {
class schema_conversion_exception final : public std::exception {
public:
    explicit schema_conversion_exception(std::string msg) noexcept
      : msg_(std::move(msg)) {}

    const char* what() const noexcept final { return msg_.c_str(); }

private:
    std::string msg_;
};
/**
 * Class representing an outcome of schema conversion. If schema validation
 * failed the outcome will contain an error. The type is simillar to the Either
 * type idea, it either contains a value or result.
 */
template<typename SchemaT>
using conversion_outcome = result<SchemaT, schema_conversion_exception>;

class value_conversion_exception final : public std::exception {
public:
    explicit value_conversion_exception(std::string msg) noexcept
      : msg_(std::move(msg)) {}

    const char* what() const noexcept final { return msg_.c_str(); }

private:
    std::string msg_;
};

using value_outcome = result<iceberg::value, value_conversion_exception>;
using optional_value_outcome
  = result<std::optional<iceberg::value>, value_conversion_exception>;

}; // namespace datalake
