/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/unicode.h"

#include <cstdlib>
#include <utf8proc.h>

namespace iceberg {

namespace {
std::string case_fold(std::string_view s) {
    uint8_t* result = nullptr;
    const utf8proc_ssize_t len = utf8proc_map(
      reinterpret_cast<const uint8_t*>(s.data()),
      static_cast<utf8proc_ssize_t>(s.size()),
      &result,
      static_cast<utf8proc_option_t>(
        UTF8PROC_CASEFOLD | UTF8PROC_COMPOSE | UTF8PROC_NULLTERM));
    if (len < 0) {
        return std::string{s};
    }
    std::string out(reinterpret_cast<char*>(result), static_cast<size_t>(len));
    // utf8proc_map allocates the output buffer; we own it and must free it.
    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc)
    free(result);
    return out;
}
} // namespace

bool names_equal(
  std::string_view a, std::string_view b, field_name_comparison norm) {
    if (norm == field_name_comparison::lower_case) {
        return case_fold(a) == case_fold(b);
    }
    return a == b;
}

} // namespace iceberg
