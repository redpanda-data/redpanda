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

#include "iceberg/field_name_comparison.h"

#include <string_view>

namespace iceberg {

/// Return true if \p a and \p b are equal under \p norm.
///
/// For field_name_comparison::lower_case both sides are Unicode case-folded
/// (UTF8PROC_CASEFOLD | COMPOSE) before comparison. This handles all Unicode
/// case mappings including 1:many (e.g. İ U+0130 → i + U+0307 COMBINING DOT
/// ABOVE) and is fully deterministic regardless of system locale.
bool names_equal(
  std::string_view a, std::string_view b, field_name_comparison norm);

} // namespace iceberg
