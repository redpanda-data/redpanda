/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "pandaproxy/schema_registry/types.h"
#include "serde/rw/envelope.h"

#include <optional>

namespace datalake {

// Uniquely identifies the structure of a record component schema as it exists
// in the schema registry.
struct schema_identifier
  : serde::
      envelope<schema_identifier, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    auto serde_fields() { return std::tie(schema_id, protobuf_offsets); }

    pandaproxy::schema_registry::schema_id schema_id;
    std::optional<std::vector<int32_t>> protobuf_offsets;
};

// The components required to build the Iceberg schema of a record.
struct record_schema_components
  : serde::envelope<
      record_schema_components,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    auto serde_fields() { return std::tie(key_identifier, val_identifier); }

    std::optional<schema_identifier> key_identifier;
    std::optional<schema_identifier> val_identifier;
};

} // namespace datalake
