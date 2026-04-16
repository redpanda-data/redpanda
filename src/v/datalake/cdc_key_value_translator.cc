/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/cdc_key_value_translator.h"

#include "datalake/logger.h"
#include "datalake/table_definition.h"

namespace datalake {

record_type
cdc_key_value_translator::build_type(std::optional<shared_resolved_type_t>) {
    // Identical schema to key_value_translator. The key column lives
    // inside the redpanda system struct as redpanda.key.
    auto ret_type = schemaless_struct_type();
    ret_type.fields.emplace_back(
      iceberg::nested_field::create(
        schemaless_next_field_id,
        "value",
        iceberg::field_required::no,
        iceberg::binary_type{}));
    return record_type{
      .comps = record_schema_components{
        .key_identifier = std::nullopt,
        .val_identifier = std::nullopt,
      },
      .type = std::move(ret_type),
      // The delete key is redpanda.key. The dotted name triggers
      // nested field lookup via find_field_by_name.
      .key_field_names = chunked_vector<ss::sstring>{"redpanda.key"},
    };
}

ss::future<checked<translated_record, record_translator::errc>>
cdc_key_value_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  const std::optional<shared_resolved_type_t>& val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    if (val_type.has_value()) {
        vlog(
          datalake_log.error,
          "Must not have parsed schema when using cdc_key_value mode");
        co_return errc::unexpected_schema;
    }

    // Build the delete key from the record key.
    std::optional<iceberg::struct_value> delete_key;
    if (key.has_value()) {
        iceberg::struct_value dk;
        dk.fields.emplace_back(iceberg::binary_value(key->copy()));
        delete_key = std::move(dk);
    }

    // Tombstone: null value means pure delete.
    if (!parsable_val.has_value()) {
        co_return translated_record{
          .data_row = std::nullopt,
          .delete_key = std::move(delete_key),
        };
    }

    // Data row identical to key_value_translator.
    auto system_data = build_rp_struct(
      pid, o, std::move(key), ts, ts_t, headers);
    iceberg::struct_value ret_data;
    ret_data.fields.emplace_back(std::move(system_data));
    ret_data.fields.emplace_back(
      iceberg::binary_value(std::move(*parsable_val)));

    co_return translated_record{
      .data_row = std::move(ret_data),
      .delete_key = std::move(delete_key),
    };
}

} // namespace datalake
