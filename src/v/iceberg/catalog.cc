/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/catalog.h"

#include "iceberg/logger.h"

#include <fmt/format.h>

namespace iceberg {

std::ostream& operator<<(std::ostream& o, catalog::errc e) {
    switch (e) {
    case catalog::errc::io_error:
        return o << "catalog::errc::io_error";
    case catalog::errc::timedout:
        return o << "catalog::errc::timedout";
    case catalog::errc::unexpected_state:
        return o << "catalog::errc::unexpected_state";
    case catalog::errc::shutting_down:
        return o << "catalog::errc::shutting_down";
    case catalog::errc::already_exists:
        return o << "catalog::errc::already_exists";
    case catalog::errc::not_found:
        return o << "catalog::errc::not_found";
    }
}

ss::future<checked<table_metadata, catalog::errc>>
catalog::load_or_create_table(
  const table_identifier& table_ident,
  const struct_type& type,
  const partition_spec& spec) {
    auto load_res = co_await load_table(table_ident);
    if (load_res.has_value()) {
        co_return std::move(load_res.value());
    }
    if (load_res.error() != iceberg::catalog::errc::not_found) {
        vlog(
          log.warn,
          "Iceberg table {} failed to load: {}",
          table_ident,
          load_res.error());
        co_return load_res.error();
    }
    vlog(
      log.info,
      "Iceberg table {} not found in catalog, creating new table",
      table_ident);
    schema schema{
      .schema_struct = type.copy(),
      .schema_id = schema::unassigned_id,
      .identifier_field_ids = {},
    };
    schema.assign_fresh_ids();
    auto create_res = co_await create_table(table_ident, schema, spec);
    if (create_res.has_value()) {
        co_return std::move(create_res.value());
    }
    if (create_res.error() != iceberg::catalog::errc::already_exists) {
        vlog(
          log.warn,
          "Iceberg table {} failed to create: {}",
          table_ident,
          create_res.error());
        co_return create_res.error();
    }
    load_res = co_await load_table(table_ident);
    if (load_res.has_value()) {
        co_return std::move(load_res.value());
    }
    // If it fails to load even after creating, regardless of what it is, just
    // fail the request.
    vlog(
      log.warn,
      "Iceberg table {} failed to load after creating",
      table_ident,
      load_res.error());
    co_return load_res.error();
}

} // namespace iceberg
