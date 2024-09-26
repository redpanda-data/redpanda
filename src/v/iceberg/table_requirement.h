// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "utils/uuid.h"

namespace iceberg::table_requirement {

struct assert_create {};

struct assert_table_uuid {
    uuid_t uuid;
};

struct last_assigned_field_match {
    nested_field::id_t last_assigned_field_id;
};

struct assert_current_schema_id {
    schema::id_t current_schema_id;
};

struct assert_last_assigned_partition_id {
    partition_field::id_t last_assigned_partition_id;
};

struct assert_ref_snapshot_id {
    ss::sstring ref;
    std::optional<snapshot_id> snapshot_id;
};

// TODO: all other requirement types.

// Represents a constraint that must be checked by the catalog before
// performing a given update.
using requirement = std::variant<
  assert_create,
  assert_current_schema_id,
  assert_ref_snapshot_id,
  assert_table_uuid,
  last_assigned_field_match,
  assert_last_assigned_partition_id>;

} // namespace iceberg::table_requirement
