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

#include <concepts>
#include <limits>
#include <type_traits>

namespace bounds {
template<std::signed_integral TargetIntegral, std::signed_integral SIntegral>
bool within_limits(const SIntegral val) {
    return val >= std::numeric_limits<TargetIntegral>::min()
           && val <= std::numeric_limits<TargetIntegral>::max();
}

template<
  std::unsigned_integral TargetIntegral,
  std::unsigned_integral UIntegral>
bool within_limits(const UIntegral val) {
    return val <= std::numeric_limits<TargetIntegral>::max();
}

template<std::signed_integral TargetIntegral, std::unsigned_integral UIntegral>
bool within_limits(const UIntegral val) {
    using Unsigned = std::make_unsigned_t<TargetIntegral>;
    return val
           <= static_cast<Unsigned>(std::numeric_limits<TargetIntegral>::max());
}

template<std::unsigned_integral TargetIntegral, std::signed_integral SIntegral>
bool within_limits(const SIntegral val) {
    using Unsigned = std::make_unsigned_t<SIntegral>;
    return val >= 0
           && static_cast<Unsigned>(val)
                <= std::numeric_limits<TargetIntegral>::max();
}

template<std::integral TargetIntegral, std::integral Integral>
bool outside_limits(const Integral val) {
    return !within_limits<TargetIntegral>(val);
}

} // namespace bounds
