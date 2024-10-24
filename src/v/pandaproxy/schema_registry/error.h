/*
 * Copyright 2021 Redpanda Data, Inc.
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
    schema_empty,
    schema_incompatible,
    schema_version_invalid,
    subject_not_found,
    subject_version_not_found,
    subject_soft_deleted,
    subject_not_deleted,
    subject_version_soft_deleted,
    subject_version_not_deleted,
    compatibility_not_found,
    mode_not_found,
    subject_version_operation_not_permitted,
    subject_version_has_references,
    subject_version_schema_id_already_exists,
    subject_schema_invalid,
    write_collision,
    topic_parse_error,
    compatibility_level_invalid,
    mode_invalid,
};

std::error_code make_error_code(error_code);

} // namespace pandaproxy::schema_registry

namespace std {

template<>
struct is_error_code_enum<pandaproxy::schema_registry::error_code>
  : true_type {};

} // namespace std
