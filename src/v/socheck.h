/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <absl/hash/hash.h>

#include <cstdlib>
#include <random>

namespace socheck {

struct socheck_data {
    int inline_global;
    int anon_global;
    int* static_var_inline_fn_ptr;
    int static_var_inline_fn_value;
    int static_var_static_fn_value;
    size_t absl_hash;
};

inline const int socheck_global = std::rand();

inline int& socheck_inline_fn() {
    static int value = std::rand();
    return value;
}

static int& socheck_static_fn() {
    static int value = std::rand();
    return value;
}

static constexpr int hash_input = 12345;

namespace { // NOLINT

inline const int socheck_anon_global = std::rand();

[[maybe_unused]] socheck_data socheck_get_data() { // NOLINT
    return {
      .inline_global = socheck_global,
      .anon_global = socheck_anon_global,
      .static_var_inline_fn_ptr = &socheck_inline_fn(),
      .static_var_inline_fn_value = socheck_inline_fn(),
      .static_var_static_fn_value = socheck_static_fn(),
      .absl_hash = absl::HashOf(hash_input)};
}

} // namespace

} // namespace socheck
