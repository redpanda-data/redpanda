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
#include "iceberg/values.h"

namespace iceberg {
class conversion_exception final : public std::exception {
public:
    explicit conversion_exception(std::string msg) noexcept
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
using conversion_outcome = result<SchemaT, conversion_exception>;

class value_conversion_exception final : public std::exception {
public:
    explicit value_conversion_exception(std::string msg) noexcept
      : msg_(std::move(msg)) {}

    const char* what() const noexcept final { return msg_.c_str(); }

private:
    std::string msg_;
};

template<typename T>
using basic_value_outcome = result<T, value_conversion_exception>;

using value_outcome = result<iceberg::value, value_conversion_exception>;
using optional_value_outcome
  = result<std::optional<iceberg::value>, value_conversion_exception>;

}; // namespace iceberg
