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
#include "iceberg/datatypes.h"

#include <seastar/util/bool_class.hh>

namespace iceberg {

enum class compat_errc {
    mismatch,
};

using type_promoted = ss::bool_class<struct type_promoted_tag>;
using type_check_result = checked<type_promoted, compat_errc>;

/**
   check_types - Performs a basic type check between two Iceberg field types,
   enforcing the Primitive Type Promotion policy laid out in
   https://iceberg.apache.org/spec/#schema-evolution

   - For non-primitive types, checks strict equality - i.e. struct == struct,
     list != map
   - Unwraps and compares input types, returns the result. Does not account for
     nesting.

   @param src  - The type of some field in an existing schema
   @param dest - The type of some field in a new schema, possibly compatible
                 with src.
   @return The result of the type check:
             - type_promoted::yes - if src -> dest is a valid type promotion
               e.g. int -> long
             - type_promoted::no  - if src == dest
             - compat_errc::mismatch - if src != dest but src -> dest is not
               permitted e.g. int -> string or struct -> list
 */
type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest);

} // namespace iceberg
