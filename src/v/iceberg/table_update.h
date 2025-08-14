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

#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <variant>

namespace iceberg::table_update {

struct add_schema {
    schema schema;
    std::optional<nested_field::id_t> last_column_id;
    add_schema copy() const {
        return {
          .schema = schema.copy(),
          .last_column_id = last_column_id,
        };
    }
};

struct set_current_schema {
    static inline constexpr schema::id_t last_added{-1};

    schema::id_t schema_id;
    set_current_schema copy() const {
        return {
          .schema_id = schema_id,
        };
    }
};

struct add_spec {
    partition_spec spec;
    add_spec copy() const {
        return {
          .spec = spec.copy(),
        };
    }
};

struct set_default_spec {
    partition_spec::id_t spec_id;
    set_default_spec copy() const { return *this; }
};

struct add_snapshot {
    snapshot snapshot;
    add_snapshot copy() const {
        return {
          .snapshot = snapshot,
        };
    }
};

struct remove_snapshots {
    chunked_vector<snapshot_id> snapshot_ids;
    remove_snapshots copy() const {
        return {.snapshot_ids = snapshot_ids.copy()};
    }
};

struct remove_snapshot_ref {
    ss::sstring ref_name;
    remove_snapshot_ref copy() const { return {.ref_name = ref_name}; }
};

struct set_snapshot_ref {
    ss::sstring ref_name;
    snapshot_reference ref;

    set_snapshot_ref copy() const {
        return {
          .ref_name = ref_name,
          .ref = ref,
        };
    }
};

// TODO: not yet implemented
// - assign_uuid
// - upgrade_format_version
// - add_sort_order
// - set_default_sort_order
// - set_location
// - set_properties
// - remove_properties

// Representation of a table update to be sent to the Iceberg catalog.
using update = std::variant<
  add_schema,
  set_current_schema,
  add_spec,
  set_default_spec,
  add_snapshot,
  remove_snapshots,
  remove_snapshot_ref,
  set_snapshot_ref>;

} // namespace iceberg::table_update
