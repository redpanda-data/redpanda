// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
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
};

struct set_current_schema {
    schema::id_t schema_id;
};

struct add_spec {
    partition_spec spec;
};

struct add_snapshot {
    snapshot snapshot;
};

struct remove_snapshots {
    chunked_vector<snapshot_id> snapshot_ids;
};

struct set_snapshot_ref {
    ss::sstring ref_name;
    snapshot_reference ref;
};

// TODO: not yet implemented
// - assign_uuid
// - upgrade_format_version
// - set_default_spec
// - add_sort_order
// - set_default_sort_order
// - remove_snapshot_ref
// - set_location
// - set_properties
// - remove_properties

// Representation of a table update to be sent to the Iceberg catalog.
using update = std::variant<
  add_schema,
  set_current_schema,
  add_spec,
  add_snapshot,
  remove_snapshots,
  set_snapshot_ref>;

} // namespace iceberg::table_update
