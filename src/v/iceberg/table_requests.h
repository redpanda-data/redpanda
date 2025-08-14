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

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
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
    std::optional<bool> stage_create = false;

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
