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

#include "base/format_to.h"
#include "bytes/iobuf.h"

#include <seastar/util/variant_utils.hh>

#include <variant>

namespace lsm {

// The result of a `get` operation interally.
class lookup_result {
    struct missing_t {
        bool operator==(const missing_t&) const = default;
    };
    struct tombstone_t {
        bool operator==(const tombstone_t&) const = default;
    };

public:
    static lookup_result missing() { return lookup_result(missing_t{}); }
    static lookup_result tombstone() { return lookup_result(tombstone_t{}); }
    static lookup_result value(iobuf b) { return lookup_result(std::move(b)); }

    bool is_missing() const {
        return std::holds_alternative<missing_t>(_value);
    }
    bool is_tombstone() const {
        return std::holds_alternative<tombstone_t>(_value);
    }
    std::optional<iobuf> value() {
        return ss::visit(
          _value,
          [](iobuf& buf) { return std::make_optional(buf.share()); },
          [](const auto&) -> std::optional<iobuf> { return std::nullopt; });
    }
    std::optional<iobuf> take_value() && {
        return ss::visit(
          _value,
          [](iobuf& buf) { return std::make_optional(std::move(buf)); },
          [](const auto&) -> std::optional<iobuf> { return std::nullopt; });
    }

    bool operator==(const lookup_result&) const = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return ss::visit(
          _value,
          [&it](const iobuf& buf) {
              return fmt::format_to(it, "{{value:{}}}", buf);
          },
          [&it](const missing_t&) { return fmt::format_to(it, "{{missing}}"); },
          [&it](const tombstone_t&) {
              return fmt::format_to(it, "{{tombstone}}");
          });
    }

private:
    explicit lookup_result(std::variant<missing_t, tombstone_t, iobuf> v)
      : _value(std::move(v)) {}

    std::variant<missing_t, tombstone_t, iobuf> _value;
};

} // namespace lsm
