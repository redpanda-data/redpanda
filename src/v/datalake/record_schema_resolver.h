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

#include "base/seastarx.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/future.hh>

namespace schema {
class registry;
} // namespace schema

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace datalake {

// Represents an object that can be converted into an Iceberg schema.
// NOTE: these aren't exactly just the schemas from the registry: Protobuf
// schemas are FileDescriptors in the registry rather than Descriptors, and
// require additional information to get the Descriptors.
using resolved_schema = std::variant<
  std::reference_wrapper<const google::protobuf::Descriptor>,
  std::reference_wrapper<const avro::ValidSchema>>;

struct resolved_type {
    // The resolved schema that corresponds to the type.
    resolved_schema schema;
    schema_identifier id;

    // The schema (and offsets, for protobuf), translated into an
    // Iceberg-compatible type. Note, the field IDs may not necessarily
    // correspond to their final IDs in the catalog.
    iceberg::field_type type;
    ss::sstring type_name;
};

struct type_and_buf {
    std::optional<resolved_type> type;

    // Part of a record field (key or value) that conforms to the given Iceberg
    // field type.
    iobuf parsable_buf;

    // Constructs a type that indicates that the record didn't have a schema or
    // there was an issue trying to parse the schema, in which case we need to
    // fall back to representing the value as a binary blob column.
    static type_and_buf make_raw_binary(iobuf buf);
};

class record_schema_resolver {
public:
    enum class errc {
        registry_error,
        translation_error,
        bad_input,
    };
    explicit record_schema_resolver(schema::registry& sr)
      : sr_(sr) {}

    ss::future<checked<type_and_buf, errc>> resolve_buf_type(iobuf b) const;

private:
    schema::registry& sr_;
};

} // namespace datalake
