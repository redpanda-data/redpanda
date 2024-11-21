/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/table_creator.h"

#include "datalake/record_translator.h"

namespace datalake {

std::ostream& operator<<(std::ostream& o, const table_creator::errc& e) {
    switch (e) {
    case table_creator::errc::incompatible_schema:
        return o << "table_creator::errc::incompatible_schema";
    case table_creator::errc::failed:
        return o << "table_creator::errc::failed";
    case table_creator::errc::shutting_down:
        return o << "table_creator::errc::shutting_down";
    }
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
    auto table_id = schema_mgr_.table_id_for_topic(topic);

    std::optional<resolved_type> val_type;
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
      topic, record_type.type);
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
