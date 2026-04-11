/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/tests/test_utils.h"

#include "datalake/record_translator.h"
#include "datalake/table_id_provider.h"

#include <optional>

namespace datalake {

iceberg::unresolved_partition_spec hour_partition_spec() {
    chunked_vector<iceberg::unresolved_partition_spec::field> fields;
    fields.push_back({
      .source_name = {"redpanda", "timestamp"},
      .transform = iceberg::hour_transform{},
      .name = "redpanda.timestamp_hour",
    });
    return {
      .fields = std::move(fields),
    };
}

direct_table_creator::direct_table_creator(
  type_resolver& tr, schema_manager& sm)
  : type_resolver_(tr)
  , schema_mgr_(sm) {}

ss::future<checked<std::nullopt_t, table_creator::errc>>
direct_table_creator::ensure_table(
  const model::topic& topic,
  model::revision_id,
  record_schema_components comps) const {
    auto table_id = table_id_provider::table_id(topic);

    std::optional<shared_resolved_type_t> val_type;
    if (comps.val_identifier) {
        auto type_res = co_await type_resolver_.resolve_identifier(
          comps.val_identifier.value());
        if (type_res.has_error()) {
            co_return errc::failed;
        }
        val_type = std::move(type_res.value());
    }

    auto record_type = default_translator{}.build_type(std::move(val_type));
    auto ensure_res = co_await schema_mgr_.ensure_table_schema(
      table_id, record_type.type, hour_partition_spec());
    if (ensure_res.has_error()) {
        switch (ensure_res.error()) {
        case schema_manager::errc::not_supported:
            co_return errc::incompatible_schema;
        case schema_manager::errc::failed:
            co_return errc::failed;
        case schema_manager::errc::shutting_down:
            co_return errc::shutting_down;
        }
    }

    co_return std::nullopt;
}

ss::future<checked<std::nullopt_t, table_creator::errc>>
direct_table_creator::ensure_dlq_table(
  const model::topic& topic, model::revision_id) const {
    auto table_id = table_id_provider::dlq_table_id(topic);

    auto record_type = key_value_translator{}.build_type(std::nullopt);
    auto ensure_res = co_await schema_mgr_.ensure_table_schema(
      table_id, record_type.type, hour_partition_spec());
    if (ensure_res.has_error()) {
        switch (ensure_res.error()) {
        case schema_manager::errc::not_supported:
            co_return errc::incompatible_schema;
        case schema_manager::errc::failed:
            co_return errc::failed;
        case schema_manager::errc::shutting_down:
            co_return errc::shutting_down;
        }
    }

    co_return std::nullopt;
}

} // namespace datalake
