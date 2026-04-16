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

#include "base/seastarx.h"
#include "iceberg/catalog_errors.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "iceberg/table_identifier.h"
#include "iceberg/transaction.h"

#include <seastar/core/future.hh>

namespace iceberg {

class catalog {
public:
    using errc = catalog_errc;

    virtual ~catalog() = default;

    // Creates a table with the given metadata.
    //
    // Returns the resulting table_metadata. Callers are free to use the
    // returned table_metadata to construct transactions.
    virtual ss::future<checked<table_metadata, errc>> create_table(
      const table_identifier& table_ident,
      const schema& schema,
      const partition_spec& spec) = 0;

    // Gets and returns the resulting table_metadata. Callers are free to use
    // the returned table_metadata to construct transactions.
    virtual ss::future<checked<table_metadata, errc>>
    load_table(const table_identifier& table_ident) = 0;

    ss::future<checked<table_metadata, errc>> load_or_create_table(
      const table_identifier& table_ident,
      const struct_type& type,
      const unresolved_partition_spec& spec);

    // Drops the table from the catalog. If `purge` is true, will also delete
    // associated data and metadata from cloud storage.
    virtual ss::future<checked<void, errc>>
    drop_table(const table_identifier& table_ident, bool purge) = 0;

    // Commits the given transaction to the catalog.
    //
    // Note that regardless of whether this succeeds or fails, the resulting
    // table_metadata may not match exactly with the transaction's state, e.g.
    // because the underlying table changed before committing but didn't break
    // any of the transactional requirements.
    //
    // Success does mean that the updates made their way to the table, but
    // failure doesn't necessarily mean that the transaction was not committed.
    //
    // Callers are expected to use the identifier used when creating or loading
    // the table.
    virtual ss::future<checked<table_metadata, errc>>
    commit_txn(const table_identifier& table_ident, transaction) = 0;

    // Probes the catalog for connectivity and credential validity.
    virtual ss::future<checked<void, catalog_describe_error>>
    describe_catalog() = 0;

    virtual ss::future<> stop() = 0;
};

} // namespace iceberg
