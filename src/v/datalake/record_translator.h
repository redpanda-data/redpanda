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
#include "datalake/record_schema_resolver.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace datalake {

struct record_type {
    record_schema_components comps;
    iceberg::struct_type type;
    /// When set, this translator produces upsert/delete operations.
    /// These field names identify the key columns for deduplication.
    /// The multiplexer resolves them to Iceberg field IDs after the
    /// table schema is registered and IDs are assigned.
    std::optional<chunked_vector<ss::sstring>> key_field_names;
};

/// Result of translating a Kafka record. For append-only translators,
/// data_row is set and delete_key is nullopt. For CDC translators,
/// either or both may be set depending on the operation.
struct translated_record {
    /// Row to insert as data. Nullopt for pure deletes.
    std::optional<iceberg::struct_value> data_row;
    /// Key value to delete. Nullopt for pure inserts.
    std::optional<iceberg::struct_value> delete_key;
};

class record_translator {
public:
    enum class errc {
        translation_error,
        unexpected_schema,
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);

    virtual record_type
    build_type(std::optional<shared_resolved_type_t> val_type) = 0;
    virtual ss::future<checked<translated_record, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<shared_resolved_type_t>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) = 0;
    virtual ~record_translator() = default;
};

class key_value_translator : public record_translator {
public:
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
    ~key_value_translator() override = default;
};

class structured_data_translator : public record_translator {
public:
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
    ~structured_data_translator() override = default;
};

// Switches between key-value and structured translator, depending on if there
// is an input schema.
// XXX: this is a temporary hack for tests to pass as we transition to toggling
// mode with a topic config! Instead, callers should explicitly choose.
class default_translator : public record_translator {
public:
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
    ~default_translator() override = default;

private:
    key_value_translator kv_translator;
    structured_data_translator structured_translator;
};

} // namespace datalake
