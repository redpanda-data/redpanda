/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "iceberg/datatypes.h"

#include <unordered_map>

namespace iceberg {

/// Defines how to map and transform an arbitrary JSON path to an iceberg type
/// and value.
/// This is an intermediate representation that is Iceberg specific but shared
/// between both schema (type) and data (value) conversions.
class json_conversion_ir {
public:
    struct field_annotation;

    using struct_field_map_t
      = std::unordered_map<std::string, field_annotation>;

    /// We want to parse JSON using a streaming (StAX style) parser. We don't
    /// know in which order the fields will be encountered, so we need to have
    /// a mapping from field names to their positions in the struct where to
    /// fill the values as they are encountered.
    ///
    /// We will also use this data structure to annotate fields with how JSON
    /// contents must be parsed based on the JSON Schema annotations like
    /// [format](https://www.learnjsonschema.com/2020-12/format-annotation/format/).
    struct field_annotation {
        // Index of the field in the struct.
        size_t field_pos;

        // Nested fields indexed by their name.
        struct_field_map_t nested_fields;
    };

public:
    explicit json_conversion_ir(
      std::unique_ptr<iceberg::field_type> root,
      struct_field_map_t struct_field_map)
      : root_(std::move(root))
      , struct_field_map_(std::move(struct_field_map)) {
        vassert(root_ != nullptr, "Root type cannot be null");
    }

    json_conversion_ir(const json_conversion_ir& other)
      : root_(std::make_unique<iceberg::field_type>(make_copy(*other.root_)))
      , struct_field_map_(other.struct_field_map_) {}

    json_conversion_ir(json_conversion_ir&& other) noexcept = default;
    json_conversion_ir&
    operator=(json_conversion_ir&& other) noexcept = default;

    ~json_conversion_ir() = default;

public:
    const iceberg::field_type& root() const { return *root_; }
    const struct_field_map_t& struct_field_map() const {
        return struct_field_map_;
    }

private:
    std::unique_ptr<iceberg::field_type> root_;
    struct_field_map_t struct_field_map_;
};

/// Convert a JSON Schema IR to a iceberg conversion IR.
conversion_outcome<json_conversion_ir>
type_to_ir(const conversion::json_schema::schema&);

}; // namespace iceberg
