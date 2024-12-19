// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/compatibility.h"

#include "iceberg/datatypes.h"

#include <absl/container/btree_map.h>
#include <fmt/format.h>

#include <variant>

namespace iceberg {

namespace {
struct primitive_type_promotion_policy_visitor {
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const iceberg::int_type&, const iceberg::long_type&) const {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::date_type&, const iceberg::timestamp_type&) const {
        return type_promoted::yes;
    }

    type_check_result
    operator()(const iceberg::float_type&, const iceberg::double_type&) {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::decimal_type& src, const iceberg::decimal_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        if ((dst.scale == src.scale && dst.precision > src.precision)) {
            return type_promoted::yes;
        }
        return compat_errc::mismatch;
    }

    type_check_result
    operator()(const iceberg::fixed_type& src, const iceberg::fixed_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        return compat_errc::mismatch;
    }
};

struct field_type_check_visitor {
    explicit field_type_check_visitor(
      primitive_type_promotion_policy_visitor policy)
      : policy(policy) {}

    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    // For non-primitives, type identity is sufficient to pass this check.
    // e.g. any two struct types will pass w/o indicating type promotion,
    // whereas a struct and, say, a list would produce a mismatch error code.
    // The member fields of two such structs (or the element fields of two
    // lists, k/v for a map, etc.) will be checked elsewhere.
    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const primitive_type& src, const primitive_type& dest) {
        return std::visit(policy, src, dest);
    }

private:
    primitive_type_promotion_policy_visitor policy;
};

} // namespace

type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest) {
    return std::visit(
      field_type_check_visitor{primitive_type_promotion_policy_visitor{}},
      src,
      dest);
}

} // namespace iceberg
