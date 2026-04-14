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

#include <stdexcept>

namespace serde::parquet {

chunked_vector<flattened_schema> flatten(const schema_element& root) {
    chunked_vector<flattened_schema> flattened;
    root.for_each([&flattened](const schema_element& elem) {
        flattened.emplace_back(
          elem.type,
          elem.repetition_type,
          elem.name(),
          elem.children.size(),
          elem.field_id,
          elem.logical_type);
    });
    return flattened;
}

namespace {

schema_element
unflatten_recursive(const chunked_vector<flattened_schema>& flat, size_t& idx) {
    if (idx >= flat.size()) {
        throw std::runtime_error(
          "malformed parquet schema: element index out of range");
    }
    const auto& elem = flat[idx];
    schema_element result;
    result.type = elem.type;
    result.repetition_type = elem.repetition_type;
    result.path.push_back(elem.name);
    result.field_id = elem.field_id;
    result.logical_type = elem.logical_type;
    ++idx;
    for (int32_t i = 0; i < elem.num_children; ++i) {
        result.children.push_back(unflatten_recursive(flat, idx));
    }
    return result;
}

} // namespace

schema_element unflatten(const chunked_vector<flattened_schema>& flat) {
    if (flat.empty()) {
        return {};
    }
    size_t idx = 0;
    auto result = unflatten_recursive(flat, idx);
    if (idx != flat.size()) {
        throw std::runtime_error(
          "malformed flattened schema: not all elements consumed");
    }
    return result;
}

} // namespace serde::parquet
