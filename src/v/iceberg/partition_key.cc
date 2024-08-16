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

bool operator==(const partition_key& lhs, const partition_key& rhs) {
    return lhs.val == rhs.val;
}

} // namespace iceberg
