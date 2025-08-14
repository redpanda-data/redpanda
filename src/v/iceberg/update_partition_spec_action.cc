/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/update_partition_spec_action.h"

#include "iceberg/logger.h"

#include <seastar/core/coroutine.hh>

namespace iceberg {

update_partition_spec_action::update_partition_spec_action(
  const table_metadata& table, unresolved_partition_spec new_spec)
  : table_(table)
  , new_spec_(std::move(new_spec))
  , logger_(
      log,
      fmt::format(
        "[table uuid: {}, location: {}]", table_.table_uuid, table_.location)) {
}

namespace {

// If the key matches, the new field will be assigned the same id.
struct field_key_t {
    nested_field::id_t source_id;
    transform transform;
    ss::sstring name;

    bool operator==(const field_key_t& other) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const field_key_t& k) {
        return H::combine(std::move(h), k.source_id, k.name);
    }
};

} // namespace

ss::future<action::action_outcome>
update_partition_spec_action::build_updates() && {
    updates_and_reqs ret;

    const auto* cur_schema = table_.get_schema(table_.current_schema_id);
    if (!cur_schema) {
        vlog(
          logger_.error,
          "can't get current schema (id: {})",
          table_.current_schema_id);
        co_return errc::unexpected_state;
    }

    const auto cur_spec_id = table_.default_spec_id;
    auto highest_spec_id = partition_spec::id_t{-1};
    chunked_hash_map<field_key_t, partition_field::id_t> field_key2id;
    for (const auto& pspec : table_.partition_specs) {
        highest_spec_id = std::max(highest_spec_id, pspec.spec_id);
        for (const auto& field : pspec.fields) {
            field_key2id[{field.source_id, field.transform, field.name}]
              = field.field_id;
        }
    }

    partition_spec resolved;
    chunked_hash_set<ss::sstring> field_names_set;
    const auto last_field_id = table_.last_partition_id;
    auto next_field_id = last_field_id + 1;
    for (const auto& field : new_spec_.fields) {
        const auto* source_field = cur_schema->schema_struct.find_field_by_name(
          field.source_name);
        if (!source_field) {
            vlog(
              logger_.warn,
              "failed to resolve source field {}",
              fmt::join(field.source_name, "."));
            co_return errc::unexpected_state;
        }

        bool added = field_names_set.insert(field.name).second;
        if (!added) {
            vlog(
              logger_.warn,
              "duplicate partition field name {} not allowed",
              field.name);
            co_return errc::unexpected_state;
        }

        partition_field resolved_field{
          .source_id = source_field->id,
          .name = field.name,
          .transform = field.transform,
        };
        field_key_t field_key{
          resolved_field.source_id,
          resolved_field.transform,
          resolved_field.name};
        auto it = field_key2id.find(field_key);
        if (it != field_key2id.end()) {
            resolved_field.field_id = it->second;
        } else {
            resolved_field.field_id = next_field_id;
            next_field_id += 1;
        }
        resolved.fields.push_back(std::move(resolved_field));
    }

    for (const auto& pspec : table_.partition_specs) {
        if (pspec.fields == resolved.fields) {
            if (pspec.spec_id == table_.default_spec_id) {
                // no-op
                co_return updates_and_reqs{};
            }
            // New spec matches an existing one. Just set it as default.
            ret.updates.push_back(
              table_update::set_default_spec{pspec.spec_id});
            ret.requirements.push_back(
              table_requirement::assert_default_spec_id{cur_spec_id});
            co_return ret;
        }
    }

    // we need to add the spec to the table
    resolved.spec_id = partition_spec::id_t{highest_spec_id() + 1};
    ret.updates.push_back(table_update::add_spec{.spec = std::move(resolved)});
    ret.requirements.push_back(
      table_requirement::assert_current_schema_id{table_.current_schema_id});
    ret.requirements.push_back(
      table_requirement::assert_last_assigned_partition_id{last_field_id});

    // set newly added spec as the default one.
    ret.updates.push_back(
      table_update::set_default_spec{partition_spec::unassigned_id});
    ret.requirements.push_back(
      table_requirement::assert_default_spec_id{cur_spec_id});

    co_return ret;
}

} // namespace iceberg
