/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "iceberg/datatypes.h"

namespace datalake {

/// Extract the inner table schema from a Debezium CDC envelope struct
/// type. Returns the "after" field's struct with all fields made optional,
/// merged into the standard schemaless struct (with redpanda system columns).
/// Used by both the translator and the coordinator to agree on the table
/// schema.
iceberg::struct_type
debezium_envelope_to_table_type(const iceberg::struct_type& envelope_type);

/// Translates Debezium CDC envelope records into Iceberg rows with
/// insert/upsert/delete semantics based on the envelope's `op` field.
class debezium_translator : public record_translator {
public:
    explicit debezium_translator(type_resolver& key_resolver);

    record_type
    build_type(std::optional<shared_resolved_type_t> val_type) override;

    ss::future<checked<translated_record, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<shared_resolved_type_t>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) override;

    ~debezium_translator() override = default;

private:
    type_resolver& _key_resolver;
    std::optional<chunked_vector<ss::sstring>> _cached_key_field_names;
};

} // namespace datalake
