/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"

#include <fmt/core.h>

#include <iosfwd>

namespace iceberg {

/// Whether field names are compared case-insensitively when matching against
/// the Iceberg catalog schema. Some catalogs (e.g. AWS Glue) return field
/// names with inconsistent casing relative to what was originally written,
/// so exact-match comparisons fail.
enum class field_name_comparison : uint8_t {
    /// Exact (case-sensitive) comparison.
    verbatim = 0,
    /// Case-insensitive comparison (Unicode lowercase via boost::locale).
    lower_case = 1,
};

fmt::iterator format_to(field_name_comparison, fmt::iterator);
std::istream& operator>>(std::istream&, field_name_comparison&);

} // namespace iceberg
