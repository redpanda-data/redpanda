/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <system_error>

namespace pandaproxy::schema_registry {

enum class error_code {
    // 0 is success
    schema_id_not_found = 1,
    schema_invalid,
    subject_not_found,
    subject_version_not_found,
};

std::error_code make_error_code(error_code);

} // namespace pandaproxy::schema_registry

namespace std {

template<>
struct is_error_code_enum<pandaproxy::schema_registry::error_code>
  : true_type {};

} // namespace std
