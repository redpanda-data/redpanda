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

#include "absl/time/time.h"
#include "bytes/iobuf.h"
#include "serde/protobuf/field_mask.h"
#include "strings/static_str.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <span>
#include <string_view>
#include <variant>

namespace serde::pb {

// A type erased value of a protobuf enum.
//
// This allows generic handling of enum values and access to both the numeric
// value and the string name.
struct raw_enum_value {
    int32_t number;
    static_str name;

    bool operator==(const raw_enum_value&) const = default;
};

class base_message;

// A generic field within a proto. Can be used for generic operations on a
// protobuf.
struct field {
    // A base class for a wrapper around a map value.
    class map_value {
    public:
        map_value() = default;
        map_value(const map_value&) = delete;
        map_value(map_value&&) = delete;
        map_value& operator=(const map_value&) = delete;
        map_value& operator=(map_value&&) = delete;
        virtual ~map_value() = default;
    };

    // A base class for a wrapper around a repeated value.
    class repeated_value {
    public:
        repeated_value() = default;
        repeated_value(const repeated_value&) = delete;
        repeated_value(repeated_value&&) = delete;
        repeated_value& operator=(const repeated_value&) = delete;
        repeated_value& operator=(repeated_value&&) = delete;
        virtual ~repeated_value() = default;
    };

    using value_t = std::variant<
      std::monostate,
      bool,
      int32_t,
      int64_t,
      uint32_t,
      uint64_t,
      ss::sstring,
      iobuf,
      float,
      double,
      raw_enum_value,
      // Well known messages are special in that we don't codegen them.
      field_mask,
      absl::Time,
      absl::Duration,
      // The following fields are pointers to field values.
      //
      // Be careful of the lifetime here, if the parent message
      // is moved, then this pointer *could* become invalidated.
      // However, these values will *never* be `nullptr` as unset
      // is translated into std::monostate.
      std::unique_ptr<map_value>,
      std::unique_ptr<repeated_value>,
      base_message*>;

    // The field's value.
    value_t value;

    bool operator==(const field&) const = default;
};

// The base message for all generated protobufs.
class base_message {
public:
    base_message() = default;
    base_message(const base_message&) = default;
    base_message(base_message&&) = default;
    base_message& operator=(const base_message&) = default;
    base_message& operator=(base_message&&) = default;
    virtual ~base_message() = default;

    // The fully qualified name of this protobuf.
    virtual std::string_view full_name() const = 0;
    // Lookup a field by it's path. Multiple field names are supported for
    // nested message traversal.
    //
    // Note that dynamic field lookup can modify the original message if looking
    // up a field with optional field presence. For example, if you have a proto
    // like so:
    //
    // ```
    // message YourProto {
    //   string value = 1;
    // }
    // message MyProto {
    //   oneof kind { YourProto nested = 1; }
    //   MyProto recursive = 2 [(redpanda.core.pbgen).ptr = true];
    // }
    // ```
    //
    // The following is valid (does not assert):
    // ```
    // my_proto pb;
    // vassert(!pb.has_nested(), "well by default it's unset");
    // // This returns std::monostate as the field value.
    // std::ignore = pb.lookup_field_by_path({"nested"});
    // vassert(pb.has_nested(), "has been set");
    // pb = {};
    // // This returns 0 as the field value.
    // std::ignore = pb.lookup_field_by_path({"nested", "value"});
    // vassert(pb.has_nested(), "and this is also set");
    // ```
    //
    // In order to lookup `value`, we *mutate* `pb` so that `nested` is set.
    // This is similar to the offical proto library behavior for the mutable*
    // methods.
    //
    // The same holds for field `recursive`.
    //
    // NOTE: If doing this in a potentially large loop, it's better
    // performance-wise to first convert the field path to field numbers (using
    // `convert_field_path_to_numbers`), then do the lookup from field
    // numbers. This avoids extra string comparisons.
    std::optional<field>
    lookup_field_by_path(std::span<std::string_view> field_path);
    // A virtual method to convert a field path into numbers.
    virtual std::optional<std::vector<int32_t>> convert_field_path_to_numbers(
      std::span<std::string_view> field_path) const = 0;
    // The same as `lookup_field_by_path`, except using field numbers instead of
    // a field path.
    virtual std::optional<field>
    lookup_field(std::span<const int32_t> field_numbers) = 0;
};

template<typename T>
concept Message = std::derived_from<T, serde::pb::base_message>;

} // namespace serde::pb
