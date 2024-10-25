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
#include "iceberg/values.h"

#include <seastar/core/future.hh>

namespace schema {
class registry;
} // namespace schema

namespace datalake {

struct resolved_buf {
    // The parsed schema, if any. If null, indicates that there was no schema,
    // or that the schema failed to parse, in which case the resulting value
    // should reflect the raw binary.
    std::optional<schema_identifier> schema_identifier;

    // The type of the buf, or null if there was no schema.
    //
    // NOTE: field IDs used in this type will not correspond to field IDs
    // understood by Iceberg. Callers should take this type (and any additional
    // desired fields) and assign all new field IDs.
    std::optional<iceberg::field_type> type;

    // The buffer translated to an Iceberg value.
    std::optional<iceberg::value> val;

    static resolved_buf make_raw_binary(iobuf);
};

class record_schema_resolver {
public:
    enum class errc {
        // There was a problem when calling the Schema Registry. Perhaps the
        // error can be retried.
        registry_error,
    };
    explicit record_schema_resolver(schema::registry& sr)
      : sr_(sr) {}

    // Resolves the Iceberg schema from the buffer, if any, and returns the
    // translated structured value.
    //
    // If there is a deterministic problem resolving the schema, returns the
    // buffer as if the schema is a simple binary field.
    ss::future<checked<resolved_buf, errc>> resolve_buf_schema(iobuf b) const;

private:
    schema::registry& sr_;
};

} // namespace datalake
