/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/container/btree_set.h"
#include "base/outcome.h"
#include "container/chunked_hash_map.h"
#include "iceberg/datatypes.h"
#include "utils/named_type.h"

namespace iceberg {

struct schema {
    enum class errc {
        missing_id,
        null_field,
    };
    using schema_outcome = checked<std::nullopt_t, errc>;
    using id_t = named_type<int32_t, struct schema_id_tag>;
    static constexpr id_t default_id{0};
    using ids_types_map_t
      = chunked_hash_map<nested_field::id_t, const field_type*>;

    struct_type schema_struct;
    id_t schema_id{default_id};
    absl::btree_set<nested_field::id_t> identifier_field_ids;
    friend bool operator==(const schema& lhs, const schema& rhs) = default;

    // Returns a mapping from field id to the field type. If the given set of
    // ids is non-empty, returns just the types of the given ids. Otherwise,
    // returns types of all ids in the schema.
    ids_types_map_t
      ids_to_types(chunked_hash_set<nested_field::id_t> = {}) const;
    std::optional<nested_field::id_t> highest_field_id() const;

    // Assigns new IDs to new fields. Field IDs are assigned depth first in
    // each field: a parent field is assigned an ID, and then each child is
    // assigned IDs, before moving onto the next parent field. Note tha
    // fields annotated with an ID from a compatibility pass are left alone.
    // Returns an error outcome if some field was null or didn't receive an ID
    schema_outcome assign_fresh_ids(
      std::optional<nested_field::id_t> next_available = std::nullopt);

    schema copy() const {
        return schema{
          .schema_struct = schema_struct.copy(),
          .schema_id = schema_id,
          .identifier_field_ids = identifier_field_ids,
        };
    }
};

} // namespace iceberg
