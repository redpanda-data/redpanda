// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/vassert.h"
#include "model/fundamental.h"

namespace model {
/// A non-empty, bounded, closed interval of offsets [min offset, max offset].
///
/// This property helps to simplify the logic and the instructions required to
/// check for overlaps, containment, etc. It is the responsibility of the caller
/// to ensure these properties hold before constructing an instance of this
/// class.
///
/// To represent a potentially empty range, wrap it in an optional.
class bounded_offset_interval {
public:
    static bounded_offset_interval
    unchecked(model::offset min, model::offset max) noexcept {
        return {min, max};
    }

    static bounded_offset_interval
    checked(model::offset min, model::offset max) {
        if (min < model::offset(0) || max < model::offset(0) || min > max) {
            throw std::invalid_argument(fmt::format(
              "Invalid arguments for constructing a non-empty bounded offset "
              "interval: min({}) <= max({})",
              min,
              max));
        }

        return {min, max};
    }

    inline bool overlaps(const bounded_offset_interval& other) const noexcept {
        return _min <= other._max && _max >= other._min;
    }

    inline bool contains(model::offset o) const noexcept {
        return _min <= o && o <= _max;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const bounded_offset_interval& r) {
        fmt::print(o, "{{min: {}, max: {}}}", r._min, r._max);
        return o;
    }

    inline model::offset min() const noexcept { return _min; }
    inline model::offset max() const noexcept { return _max; }

private:
    bounded_offset_interval(model::offset min, model::offset max) noexcept
      : _min(min)
      , _max(max) {
#ifndef NDEBUG
        vassert(
          min >= model::offset(0), "Offset interval min({}) must be >= 0", min);
        vassert(
          min <= max,
          "Offset interval invariant not satisfied: min({}) <= max({})",
          min,
          max);
#endif
    }

    model::offset _min;
    model::offset _max;
};

} // namespace model
