/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/simulate_evolution.h"

#include "iceberg/compatibility.h"
#include "iceberg/datatypes.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"

namespace iceberg {

namespace {

void mark_all_fields_new(struct_type& st);
void clear_all_metadata(struct_type& st);

void visit_field_type(field_type& ft, auto&& field_fn) {
    std::visit(
      [&](auto& t) {
          using T = std::decay_t<decltype(t)>;
          if constexpr (std::is_same_v<T, struct_type>) {
              for (auto& f : t.fields) {
                  field_fn(*f);
                  visit_field_type(f->type, field_fn);
              }
          } else if constexpr (std::is_same_v<T, list_type>) {
              field_fn(*t.element_field);
              visit_field_type(t.element_field->type, field_fn);
          } else if constexpr (std::is_same_v<T, map_type>) {
              field_fn(*t.key_field);
              visit_field_type(t.key_field->type, field_fn);
              field_fn(*t.value_field);
              visit_field_type(t.value_field->type, field_fn);
          }
      },
      ft);
}

void mark_all_fields_new(struct_type& st) {
    for (auto& f : st.fields) {
        f->set_evolution_metadata(nested_field::is_new{});
        visit_field_type(f->type, [](nested_field& nf) {
            nf.set_evolution_metadata(nested_field::is_new{});
        });
    }
}

void clear_all_metadata(struct_type& st) {
    for (auto& f : st.fields) {
        f->meta = std::nullopt;
        visit_field_type(
          f->type, [](nested_field& nf) { nf.meta = std::nullopt; });
    }
}

} // namespace

simulation_result
simulate_evolution(chunked_vector<struct_type> schema_sequence) {
    if (schema_sequence.empty()) {
        return simulation_step_failure{
          .errc = schema_evolution_errc::invalid_state, .step = 0};
    }

    // Take schema_sequence[0] as the initial virtual table schema.
    auto accumulated = std::move(schema_sequence[0]);

    // Assign real field IDs to the initial schema.
    mark_all_fields_new(accumulated);
    schema initial_schema{.schema_struct = std::move(accumulated)};
    auto assign_res = initial_schema.assign_fresh_ids(nested_field::id_t{1});
    if (assign_res.has_error()) {
        return simulation_step_failure{
          .errc = schema_evolution_errc::invalid_state, .step = 0};
    }

    auto last_column_id = initial_schema.highest_field_id().value_or(
      nested_field::id_t{0});
    accumulated = std::move(initial_schema.schema_struct);
    clear_all_metadata(accumulated);

    const partition_spec empty_spec;

    for (size_t i = 1; i < schema_sequence.size(); ++i) {
        // Step 1: Check if writer is a compatible subset of accumulated.
        auto writer_copy = schema_sequence[i].copy();
        auto fill_res = try_fill_field_ids(accumulated, writer_copy);

        if (fill_res == ids_filled::yes) {
            // Writer is a subset (possibly with narrower-but-promotable
            // types). No schema change needed.
            continue;
        }

        // Step 2: Writer has new fields or structural differences.
        // Merge writer into a copy of accumulated.
        auto merged = accumulated.copy();
        auto merge_res = merge_struct_types(schema_sequence[i], merged);
        if (merge_res.has_error()) {
            return simulation_step_failure{
              .errc = merge_res.error(), .step = i};
        }

        // Step 3: Validate the merged schema is a valid evolution of
        // accumulated.
        auto evo_res = evolve_schema(accumulated, merged, empty_spec);
        if (evo_res.has_error()) {
            return simulation_step_failure{.errc = evo_res.error(), .step = i};
        }

        if (evo_res.value() == schema_changed::no) {
            // try_fill_field_ids said the writer can't write to the
            // accumulated schema, yet merging and evolving produced no
            // schema change. This happens when the writer re-introduces
            // a previously-dropped field with an incompatible type: the
            // merge places the field but it conflicts with the existing
            // column in the accumulated schema.
            return simulation_step_failure{
              .errc = schema_evolution_errc::incompatible, .step = i};
        }

        // Assign fresh IDs to any new fields in the merged schema.
        schema merged_schema{.schema_struct = std::move(merged)};
        auto fresh_res = merged_schema.assign_fresh_ids(
          nested_field::id_t{last_column_id() + 1});
        if (fresh_res.has_error()) {
            return simulation_step_failure{
              .errc = schema_evolution_errc::invalid_state, .step = i};
        }

        last_column_id = merged_schema.highest_field_id().value_or(
          last_column_id);
        accumulated = std::move(merged_schema.schema_struct);
        clear_all_metadata(accumulated);
    }

    return accumulated;
}

} // namespace iceberg
