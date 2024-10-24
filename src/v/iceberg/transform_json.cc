// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/transform_json.h"

#include "iceberg/json_utils.h"
#include "iceberg/transform.h"
#include "strings/string_switch.h"

#include <string>

namespace iceberg {

namespace {

struct transform_str_visitor {
    ss::sstring operator()(const identity_transform&) { return "identity"; }
    ss::sstring operator()(const bucket_transform& t) {
        return fmt::format("bucket[{}]", t.n);
    }
    ss::sstring operator()(const truncate_transform& t) {
        return fmt::format("truncate[{}]", t.length);
    }
    ss::sstring operator()(const year_transform&) { return "year"; }
    ss::sstring operator()(const month_transform&) { return "month"; }
    ss::sstring operator()(const day_transform&) { return "day"; }
    ss::sstring operator()(const hour_transform&) { return "hour"; }
    ss::sstring operator()(const void_transform&) { return "void"; }
};

} // namespace

ss::sstring transform_to_str(const transform& t) {
    return std::visit(transform_str_visitor{}, t);
}

transform transform_from_str(std::string_view s) {
    if (s.starts_with("bucket")) {
        auto n_str = extract_between('[', ']', s);
        auto n = std::stoul(ss::sstring(n_str));
        return bucket_transform{static_cast<uint32_t>(n)};
    }
    if (s.starts_with("truncate")) {
        auto len_str = extract_between('[', ']', s);
        auto len = std::stoul(ss::sstring(len_str));
        return truncate_transform{static_cast<uint32_t>(len)};
    }
    return string_switch<transform>(s)
      .match("identity", identity_transform{})
      .match("year", year_transform{})
      .match("month", month_transform{})
      .match("day", day_transform{})
      .match("hour", hour_transform{})
      .match("void", void_transform{});
}

} // namespace iceberg
