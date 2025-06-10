/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/catalog_schema_manager.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "iceberg/compatibility.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/table_identifier.h"
#include "iceberg/transaction.h"

namespace datalake {

namespace {

// NB: Use the concrete type not the alias iceberg::table_properties_t
// so changes to the alias can't silently destroy the deep copy semantics
// of this function.
chunked_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq>
copy_properties(
  const chunked_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq>&
    props) {
    chunked_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq> result;
    for (const auto& [key, value] : props) {
        result.emplace(key, value);
    }
    return result;
}

schema_manager::errc log_and_convert_catalog_err(
  iceberg::catalog::errc e, std::string_view log_msg) {
    switch (e) {
    case iceberg::catalog::errc::shutting_down:
        vlog(datalake_log.debug, "{}: {}", log_msg, e);
        return schema_manager::errc::shutting_down;
    case iceberg::catalog::errc::timedout:
    case iceberg::catalog::errc::not_found:
    case iceberg::catalog::errc::io_error:
    case iceberg::catalog::errc::unexpected_state:
    case iceberg::catalog::errc::already_exists:
        vlog(datalake_log.warn, "{}: {}", log_msg, e);
        return schema_manager::errc::failed;
    }
}
enum class fill_errc {
    // There is a mismatch in a field's type, name, or required.
    invalid_schema,
    // We couldn't fill all the columns, but the ones we could all matched.
    // Or one or more columns were type-promoted (legally), so we'll need to
    // push an update into the catalog.
    schema_evolution_needed,
};

// Performs a simultaneous, depth-first iteration through fields of the two
// schemas, filling dest's field IDs with those from the source. Returns
// successfully if all the field IDs in the destination type are filled.
checked<std::nullopt_t, fill_errc>
fill_field_ids(iceberg::struct_type& dest, const iceberg::struct_type& source) {
    using namespace iceberg;
    if (auto fill_res = try_fill_field_ids(source, dest);
        fill_res.has_error()) {
        vlog(
          datalake_log.warn,
          "Schema compatibility error: '{}'\n",
          fill_res.error());
        return fill_errc::invalid_schema;
    } else if (!fill_res.value()) {
        // Some fields left without IDs
        return fill_errc::schema_evolution_needed;
    }
    // We successfully filled all the fields in the destination.
    return std::nullopt;
}

// Performs a simultaneous, depth-first iteration through fields of the two
// schemas, filling dest's field IDs with those from the source and checking
// compatibility between the two schemas along the way. Returns successfully if
// the schemas are identical. An error indicates that the schemas differ, in
// which case the errc indicates whether they are compatible. source can be
// correctly evolved to dest.
// NOTE: pass the source struct by value to create a clear barrier between the
// traversal and whatever (probably cached) metadata the source struct was
// pulled from.
// NOTE: Post processing is required to assign IDs to any destination fields not
// found in source (i.e. new fields).
checked<std::nullopt_t, fill_errc> check_schema_compat(
  iceberg::struct_type& dest,
  iceberg::struct_type source,
  const iceberg::partition_spec& spec) {
    using namespace iceberg;
    if (auto evo_res = evolve_schema(source, dest, spec); evo_res.has_error()) {
        vlog(
          datalake_log.warn,
          "Schema compatibility error: '{}'\n",
          evo_res.error());
        return fill_errc::invalid_schema;
    } else if (evo_res.value()) {
        // The schemas are compatible but table metadata needs updating to
        // reflect dest
        return fill_errc::schema_evolution_needed;
    }
    // The schemas are identical
    return std::nullopt;
}
} // namespace

std::ostream& operator<<(std::ostream& o, const schema_manager::errc& e) {
    switch (e) {
    case schema_manager::errc::not_supported:
        return o << "schema_manager::errc::not_supported";
    case schema_manager::errc::failed:
        return o << "schema_manager::errc::failed";
    case schema_manager::errc::shutting_down:
        return o << "schema_manager::errc::shutting_down";
    }
}

bool schema_manager::table_info::fill_registered_ids(
  iceberg::struct_type& type) {
    auto fill_res = fill_field_ids(type, schema.schema_struct);
    return !fill_res.has_error();
}

ss::future<checked<std::nullopt_t, schema_manager::errc>>
simple_schema_manager::ensure_table_schema(
  const iceberg::table_identifier& table_id,
  const iceberg::struct_type& desired_type,
  const iceberg::unresolved_partition_spec& partition_spec) {
    iceberg::schema s{
      .schema_struct = desired_type.copy(),
      .schema_id = {},
      .identifier_field_ids = {},
    };
    if (auto schm_res = s.assign_fresh_ids(); schm_res.has_error()) {
        co_return errc::failed;
    }

    auto resolve_res = iceberg::partition_spec::resolve(
      partition_spec, s.schema_struct);
    if (!resolve_res) {
        co_return errc::failed;
    }

    // TODO: check schema compatibility
    table_info_by_id.insert_or_assign(
      table_id.copy(),
      table_info{
        .id = table_id.copy(),
        .schema = std::move(s),
        .partition_spec = std::move(resolve_res.value()),
        .location = iceberg::uri(fmt::format(
          "{}/{}",
          table_location_prefix_(),
          fmt::join(table_id.ns, "/"),
          table_id.table)),
        .properties = std::nullopt,
      });

    co_return std::nullopt;
}

ss::future<checked<schema_manager::table_info, schema_manager::errc>>
simple_schema_manager::get_table_info(
  const iceberg::table_identifier& table_id,
  std::optional<std::reference_wrapper<iceberg::struct_type>>) {
    auto it = table_info_by_id.find(table_id);
    if (it == table_info_by_id.end()) {
        co_return errc::failed;
    }
    co_return table_info{
      .id = it->second.id.copy(),
      .schema = it->second.schema.copy(),
      .partition_spec = it->second.partition_spec.copy(),
      .location = it->second.location,
      .properties = it->second.properties.transform(copy_properties),
    };
}

ss::future<checked<std::nullopt_t, schema_manager::errc>>
catalog_schema_manager::ensure_table_schema(
  const iceberg::table_identifier& table_id,
  const iceberg::struct_type& desired_type,
  const iceberg::unresolved_partition_spec& partition_spec) {
    auto gh = maybe_gate();
    if (gh.has_error()) {
        co_return gh.error();
    }
    auto load_res = co_await catalog_.load_or_create_table(
      table_id, desired_type, partition_spec);
    if (load_res.has_error()) {
        co_return log_and_convert_catalog_err(
          load_res.error(), fmt::format("Error loading table {}", table_id));
    }

    iceberg::transaction txn(std::move(load_res.value()));

    // Check schema compatibility
    auto type_copy = desired_type.copy();
    auto get_res = get_ids_from_table_meta(table_id, txn.table(), type_copy);
    if (get_res.has_error()) {
        co_return get_res.error();
    }

    if (!get_res.value()) {
        // The desired schema is backwards compatible with the current table
        // schema. Add the schema to the table.
        auto update_res = co_await txn.set_schema(iceberg::schema{
          .schema_struct = std::move(type_copy),
          .schema_id = iceberg::schema::unassigned_id,
          .identifier_field_ids = {},
        });
        if (update_res.has_error()) {
            auto msg = fmt::format(
              "Failed trying to apply schema update to table {}: {}",
              table_id,
              update_res.error());
            switch (update_res.error()) {
            case iceberg::action::errc::shutting_down:
                vlog(datalake_log.debug, "{}", msg);
                co_return errc::shutting_down;
            case iceberg::action::errc::io_failed:
            case iceberg::action::errc::unexpected_state:
                vlog(datalake_log.warn, "{}", msg);
                co_return errc::failed;
            }
        }
    }

    auto set_spec_res = co_await txn.set_partition_spec(partition_spec.copy());
    if (set_spec_res.has_error()) {
        co_return errc::failed;
    }

    if (!txn.is_noop()) {
        auto commit_res = co_await catalog_.commit_txn(
          table_id, std::move(txn));
        if (commit_res.has_error()) {
            co_return log_and_convert_catalog_err(
              commit_res.error(),
              fmt::format(
                "Error while committing schema update to table {}", table_id));
        }
    }

    co_return std::nullopt;
}

ss::future<checked<schema_manager::table_info, schema_manager::errc>>
catalog_schema_manager::get_table_info(
  const iceberg::table_identifier& table_id,
  std::optional<std::reference_wrapper<iceberg::struct_type>> desired_type) {
    auto gh = maybe_gate();
    if (gh.has_error()) {
        co_return gh.error();
    }
    auto load_res = co_await catalog_.load_table(table_id);
    if (load_res.has_error()) {
        co_return log_and_convert_catalog_err(
          load_res.error(),
          fmt::format(
            "Error while reloading table {} after schema update", table_id));
    }
    const auto& table = load_res.value();

    const auto* cur_schema = desired_type.has_value()
                               ? table.get_equivalent_schema(
                                   desired_type->get())
                               : table.get_schema(table.current_schema_id);
    if (!cur_schema) {
        vlog(
          datalake_log.error,
          "Cannot find current schema {} in table {}",
          table.current_schema_id,
          table_id);
        co_return errc::failed;
    }

    auto cur_spec = table.get_partition_spec(table.default_spec_id);
    if (!cur_spec) {
        vlog(
          datalake_log.error,
          "Cannot find default partition spec {} in table {}",
          table.default_spec_id,
          table_id);
        co_return errc::failed;
    }

    co_return table_info{
      .id = table_id.copy(),
      .schema = cur_schema->copy(),
      .partition_spec = cur_spec->copy(),
      .location = table.location,
      .properties = table.properties.transform(copy_properties),
    };
}

checked<bool, schema_manager::errc>
catalog_schema_manager::get_ids_from_table_meta(
  const iceberg::table_identifier& table_id,
  const iceberg::table_metadata& table_meta,
  iceberg::struct_type& dest_type) {
    const auto* schema = table_meta.get_equivalent_schema(dest_type);
    if (schema != nullptr) {
        vlog(
          datalake_log.debug,
          "Found exact match schema: ID {}",
          schema->schema_id);
        return true;
    }

    schema = table_meta.get_schema(table_meta.current_schema_id);
    if (schema == nullptr) {
        vlog(
          datalake_log.error,
          "Cannot find current schema {} in table {}",
          table_meta.current_schema_id,
          table_id);
        return errc::failed;
    }

    const auto* cur_spec = table_meta.get_partition_spec(
      table_meta.default_spec_id);
    if (cur_spec == nullptr) {
        vlog(
          datalake_log.error,
          "Cannot find default partition spec {} in table {}",
          table_meta.default_spec_id,
          table_id);
        return errc::failed;
    }
    auto compat_res = check_schema_compat(
      dest_type, schema->schema_struct.copy(), *cur_spec);
    if (compat_res.has_error()) {
        switch (compat_res.error()) {
        case fill_errc::invalid_schema:
            vlog(datalake_log.warn, "Type mismatch with table {}", table_id);
            return errc::not_supported;
        case fill_errc::schema_evolution_needed:
            return false;
        }
    }
    return true;
}

checked<ss::gate::holder, catalog_schema_manager::errc>
catalog_schema_manager::maybe_gate() {
    auto holder = gate_.try_hold();
    if (!holder) {
        return errc::shutting_down;
    }
    return std::move(holder.value());
}

ss::future<> catalog_schema_manager::stop() { return gate_.close(); }

} // namespace datalake
