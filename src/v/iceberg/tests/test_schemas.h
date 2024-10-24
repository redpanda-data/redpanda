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

namespace iceberg {

// Expected type from test_nested_schema_json_str.
field_type test_nested_schema_type();

// Contains additional fields that are currently supported by avro:
// decimals, fixed and UUID
field_type test_nested_schema_type_avro();

// Nested schema taken from
// https://github.com/apache/iceberg-go/blob/704a6e78c13ea63f1ff4bb387f7d4b365b5f0f82/schema_test.go#L644
extern const char* test_nested_schema_json_str;

} // namespace iceberg
