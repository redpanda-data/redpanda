/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <vector>

namespace encryption {

struct field_encryption_annotation {
    std::vector<ss::sstring> path;
    ss::sstring kek_name;
    ss::sstring kms_key_id;
};

/// Parse encryption annotations from an Avro schema JSON string.
///
/// Walks the schema's record fields looking for custom properties
/// "encryption:kek_name" and "encryption:kms_key_id". Nested records
/// (including those inside unions) are traversed recursively; the returned
/// path reflects the full field path from the root record.
std::vector<field_encryption_annotation>
parse_avro_encryption_annotations(const ss::sstring& schema_json);

/// Parse encryption annotations from a JSON Schema string.
///
/// Walks the "properties" of JSON Schema objects looking for custom
/// properties "encryption:kek_name" and "encryption:kms_key_id". Nested
/// objects are traversed recursively.
std::vector<field_encryption_annotation>
parse_json_schema_encryption_annotations(const ss::sstring& schema_json);

} // namespace encryption
