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
#include "iceberg/storage_credentials.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
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

struct create_namespace_request {
    chunked_vector<ss::sstring> ns;
    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> properties;
};

struct create_namespace_response {
    chunked_vector<ss::sstring> ns;
    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> properties;
};

struct load_table_result {
    table_metadata metadata;
    std::optional<ss::sstring> metadata_location;
    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> config;
    std::optional<chunked_vector<storage_credentials>> storage_credentials;
};

struct commit_table_request {
    table_identifier identifier;
    chunked_vector<table_update::update> updates;
    chunked_vector<table_requirement::requirement> requirements;
};

struct commit_table_response {
    ss::sstring metadata_location;
    table_metadata table_metadata;
};

} // namespace iceberg
