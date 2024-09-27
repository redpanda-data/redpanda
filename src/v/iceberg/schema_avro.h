// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"

#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>

namespace iceberg {

// Translates the given field/type to the corresponding Avro schema.
// The resulting schema is annotated with Iceberg attributes (e.g. 'field-id',
// 'element-id').
avro::Schema field_to_avro(const nested_field& field);
avro::Schema struct_type_to_avro(const struct_type&, std::string_view name);

// Translates the given Avro schema into its corresponding field/type, throwing
// an exception if the schema is not a valid Iceberg schema (e.g. missing
// attributes).
nested_field_ptr
child_field_from_avro(const avro::NodePtr& parent, size_t child_idx);
field_type type_from_avro(const avro::NodePtr&);

} // namespace iceberg
