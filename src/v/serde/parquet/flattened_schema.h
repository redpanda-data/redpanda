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

#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "serde/parquet/schema.h"

#include <seastar/core/sstring.hh>

namespace serde::parquet {

/**
 * A flattened schema is the metadata representation of a parquet schema, which
 * is flattened due to Apache Thift not supporting recursion.
 */
struct flattened_schema {
    /**
     * The physical encoding of the data. Left unset for intermediate nodes.
     */
    physical_type type;

    /**
     * repetition of the field. The root of the schema does not have a
     * repetition_type. All other nodes must have one.
     *
     * (this field is ignored on the schema root).
     */
    field_repetition_type repetition_type;

    /** Name of the field in the schema. */
    ss::sstring name;

    /**
     * Nested fields.
     */
    int32_t num_children;

    /**
     * When the original schema supports field ids, this will save the
     * original field id in the parquet schema
     */
    std::optional<int32_t> field_id;

    /**
     * For leaf nodes, the logical type the physical bytes represent.
     */
    logical_type logical_type;
};

/**
 * Flatten a schema element into a form that is acceptable for serialized
 * parquet metadata.
 */
chunked_vector<flattened_schema> flatten(const schema_element& root);

} // namespace serde::parquet
