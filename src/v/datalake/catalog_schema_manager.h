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

#include "iceberg/catalog.h"
#include "iceberg/datatypes.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "iceberg/table_identifier.h"

namespace features {
class feature_table;
}

namespace datalake {

class schema_manager {
public:
    enum class errc {
        // The requested operation is not supported (e.g. unsupported schema
        // evolution).
        not_supported,
        // The operation failed because of a subsystem failure.
        failed,
        // The system is shutting down.
        shutting_down,
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);

    virtual ss::future<checked<std::nullopt_t, errc>> ensure_table_schema(
      const iceberg::table_identifier&,
      const iceberg::struct_type& desired_type,
      const iceberg::unresolved_partition_spec&) = 0;

    struct table_info {
        iceberg::table_identifier id;
        iceberg::schema schema;
        iceberg::partition_spec partition_spec;
        iceberg::uri location;
        std::optional<iceberg::table_properties_t> properties;

        // Fills the field IDs of the given type with those in the current
        // schema. Returns true on success.
        bool fill_registered_ids(iceberg::struct_type&);
    };

    virtual ss::future<checked<table_info, errc>> get_table_info(
      const iceberg::table_identifier&,
      std::optional<std::reference_wrapper<iceberg::struct_type>> desired_type
      = std::nullopt) = 0;

    virtual ss::future<> stop() = 0;
    virtual ~schema_manager() = default;
};

// Used in unit tests
class simple_schema_manager : public schema_manager {
public:
    explicit simple_schema_manager(iceberg::uri table_location_prefix = {})
      : table_location_prefix_(std::move(table_location_prefix)) {}

public:
    ss::future<checked<std::nullopt_t, schema_manager::errc>>
    ensure_table_schema(
      const iceberg::table_identifier&,
      const iceberg::struct_type& desired_type,
      const iceberg::unresolved_partition_spec&) override;

    ss::future<checked<table_info, schema_manager::errc>> get_table_info(
      const iceberg::table_identifier&,
      std::optional<std::reference_wrapper<iceberg::struct_type>> desired_type
      = std::nullopt) override;
    ss::future<> stop() final { return ss::now(); }

private:
    iceberg::uri table_location_prefix_;
    chunked_hash_map<iceberg::table_identifier, table_info> table_info_by_id;
};

// Manages interactions with the catalog when reconciling the current schema of
// a given table. This is where Redpanda should make decisions about schema
// evolution.
class catalog_schema_manager : public schema_manager {
public:
    explicit catalog_schema_manager(
      iceberg::catalog& catalog, features::feature_table* features)
      : catalog_(catalog)
      , features_(features) {}

    // Ensure the table schema is compatible with the writer struct. If the
    // table does not exist it is created, or, if the table exists and its
    // current schema doesn't include all of the fields (e.g. we are going from
    // the schemaless schema to a schema containing user fields), the table's
    // schema is updated to be compatible with the writer_struct.
    ss::future<checked<std::nullopt_t, schema_manager::errc>>
    ensure_table_schema(
      const iceberg::table_identifier&,
      const iceberg::struct_type& writer_struct_type,
      const iceberg::unresolved_partition_spec&) override;

    // Loads the table metadata for the given topic.
    ss::future<checked<table_info, schema_manager::errc>> get_table_info(
      const iceberg::table_identifier&,
      std::optional<std::reference_wrapper<iceberg::struct_type>>
        writer_struct_type = std::nullopt) override;

    // Stops the schema manager, waiting for any ongoing operations to
    // complete.
    ss::future<> stop() override;

private:
    checked<ss::gate::holder, errc> maybe_gate();
    iceberg::catalog& catalog_;
    features::feature_table* features_;
    ss::gate gate_;
};

} // namespace datalake
