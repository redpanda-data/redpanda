// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/partition_key.h"

#include "iceberg/transform_utils.h"

namespace iceberg {

partition_key partition_key::create(
  const struct_value& source_struct,
  const struct_accessor::ids_accessor_map_t& accessors,
  const partition_spec& spec) {
    auto ret_val = std::make_unique<struct_value>();
    for (const auto& partition_field : spec.fields) {
        const auto& source_id = partition_field.source_id;
        auto acc_iter = accessors.find(source_id);
        if (acc_iter == accessors.end()) {
            throw std::invalid_argument(
              fmt::format("Expected accessor for field id {}", source_id));
        }
        const auto& field_accessor = acc_iter->second;
        const auto& field_val_opt = field_accessor->get(source_struct);
        if (!field_val_opt.has_value()) {
            // All transforms must return null for a null input value.
            ret_val->fields.emplace_back(std::nullopt);
            continue;
        }
        const auto& field_val = *field_val_opt;
        const auto& transform = partition_field.transform;
        ret_val->fields.emplace_back(apply_transform(field_val, transform));
    }
    return partition_key{std::move(ret_val)};
}

partition_key partition_key::copy() const {
    auto ret_val = std::make_unique<struct_value>();
    for (const auto& partition_field : val->fields) {
        if (!partition_field.has_value()) {
            ret_val->fields.emplace_back(std::nullopt);
            continue;
        }
        // Partition keys should only be comprised of primitive values, per the
        // partitioning transformations defined by the Iceberg spec.
        const auto& partition_val = partition_field.value();
        if (!std::holds_alternative<primitive_value>(partition_val)) {
            throw std::invalid_argument(fmt::format(
              "Partition key holds unexpected non-primitive value: {}",
              partition_val));
        }
        // NOTE: primitive values all have default copy constructors.
        ret_val->fields.emplace_back(
          make_copy(std::get<primitive_value>(partition_val)));
    }
    return partition_key{std::move(ret_val)};
}

bool operator==(const partition_key& lhs, const partition_key& rhs) {
    return lhs.val == rhs.val;
}

} // namespace iceberg
