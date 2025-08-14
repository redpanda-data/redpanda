/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "src/v/bytes/hash.h" // IWYU pragma: keep

#include <variant>

namespace serde::json::test::dom {

struct null_t {
    bool operator==(const null_t&) const { return true; }
};

class value;

using json_object = chunked_hash_map<iobuf, value>;
using json_array = chunked_vector<value>;

constexpr null_t null_value{};

/// A generic JSON value.
class value {
private:
public:
    template<typename T>
    explicit value(T v)
      : _data(std::move(v)) {}

    iobuf& string() { return std::get<iobuf>(_data); }

    const auto& data() const { return _data; }

    bool operator==(const value& other) const {
        if (
          std::holds_alternative<double>(_data)
          && std::holds_alternative<double>(other._data)) {
            // TODO: Compare double values with a tolerance to avoid floating
            // point precision issues. This is temporary workaround for
            // precision loss in parsing when compared to rapidjson.

            auto almost_equal = [](double a, double b) {
                double maxXYOne = std::max({1.0, std::fabs(a), std::fabs(b)});

                return std::fabs(a - b)
                       <= 2 * std::numeric_limits<double>::epsilon() * maxXYOne;
            };

            return almost_equal(
              std::get<double>(_data), std::get<double>(other._data));
        }
        return _data == other._data;
    }

private:
    std::variant<null_t, bool, int64_t, double, iobuf, json_array, json_object>
      _data;
};

std::ostream& operator<<(std::ostream&, const value&);

} // namespace serde::json::test::dom
