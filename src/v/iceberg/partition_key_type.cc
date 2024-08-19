// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/partition_key_type.h"

#include "iceberg/partition.h"
#include "iceberg/schema.h"

namespace iceberg {

namespace {

// TODO: at some point we should restrict the source types to what is defined
// by the spec. For now, expect that the input types are allowed.
struct transform_result_type_visitor {
    explicit transform_result_type_visitor(const field_type& source)
      : source_type_(source) {}

    const field_type& source_type_;

    field_type operator()(const identity_transform&) {
        if (std::holds_alternative<primitive_type>(source_type_)) {
            return std::get<primitive_type>(source_type_);
        }
        // TODO: requires a field copy method.
        throw std::invalid_argument("Identity transform not supported");
    }
    field_type operator()(const bucket_transform&) { return int_type(); }
    field_type operator()(const truncate_transform&) {
        if (std::holds_alternative<primitive_type>(source_type_)) {
            return std::get<primitive_type>(source_type_);
        }
        // TODO: requires a field copy method.
        throw std::invalid_argument("Truncate transform not supported");
    }
    field_type operator()(const year_transform&) { return int_type(); }
    field_type operator()(const month_transform&) { return int_type(); }
    field_type operator()(const day_transform&) { return int_type(); }
    field_type operator()(const hour_transform&) { return int_type(); }
    field_type operator()(const void_transform&) {
        // TODO: the spec also says the result may also be the source type.
        return int_type();
    }
};

field_type
get_result_type(const field_type& source_type, const transform& transform) {
    return std::visit(transform_result_type_visitor{source_type}, transform);
}

} // namespace

partition_key_type partition_key_type::create(
  const partition_spec& partition_spec, const schema& schema) {
    struct_type schema_struct;
    const auto& partition_fields = partition_spec.fields;
    chunked_hash_set<nested_field::id_t> partition_field_ids;
    for (const auto& field : partition_fields) {
        partition_field_ids.emplace(field.source_id);
    }
    const auto ids_to_types = schema.ids_to_types(
      std::move(partition_field_ids));
    for (const auto& field : partition_fields) {
        const auto& source_id = field.source_id;
        const auto type_iter = ids_to_types.find(source_id);
        if (type_iter == ids_to_types.end()) {
            throw std::invalid_argument(fmt::format(
              "Expected source field ID {} to be in schema", source_id()));
        }
        const auto& source_type = *type_iter->second;
        auto result_field = nested_field::create(
          field.field_id,
          field.name,
          field_required::no,
          get_result_type(source_type, field.transform));
        schema_struct.fields.emplace_back(std::move(result_field));
    }
    return {std::move(schema_struct)};
}

} // namespace iceberg
