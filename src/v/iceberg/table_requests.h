// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

namespace iceberg {

struct create_table_request {
    ss::sstring name;
    schema schema;
    std::optional<ss::sstring> location;
    std::optional<partition_spec> partition_spec;

    // TODO: (optional) sort_order

    // If set to true, the table is not created, but table metadata is
    // initialized and returned by the catalog.
    std::optional<bool> stage_create;

    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> properties;
};

struct commit_table_request {
    chunked_vector<table_update::update> updates;
    chunked_vector<table_requirement::requirement> requirements;
};

} // namespace iceberg
