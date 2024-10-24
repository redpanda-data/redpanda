/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/flattened_schema.h"

namespace serde::parquet {

chunked_vector<flattened_schema> flatten(const schema_element& root) {
    chunked_vector<flattened_schema> flattened;
    root.for_each([&flattened](const schema_element& elem) {
        flattened.emplace_back(
          elem.type,
          elem.repetition_type,
          elem.name,
          elem.children.size(),
          elem.field_id,
          elem.logical_type);
    });
    return flattened;
}

} // namespace serde::parquet
