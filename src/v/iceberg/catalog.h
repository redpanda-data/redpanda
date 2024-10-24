// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"
#include "iceberg/transaction.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace iceberg {

struct table_identifier {
    chunked_vector<ss::sstring> ns;
    ss::sstring table;
};
std::ostream& operator<<(std::ostream& o, const table_identifier& id);

class catalog {
public:
    enum class errc {
        // There was a problem at the IO layer.
        io_error,

        // IO has timed out. Depending on the caller, may be worth retrying.
        timedout,

        // There was some unexpected state (e.g. a broken invariant in the
        // loaded metadata).
        unexpected_state,

        // There was a problem that indicates the system is shutting down. Best
        // to quiesce operation.
        shutting_down,

        // E.g. a given table already exists.
        already_exists,

        // E.g. a table is not found.
        not_found,
    };
    virtual ~catalog() = default;

    // Creates a table with the given metadata.
    //
    // Returns the resulting table_metadata. Callers are free to use the
    // returned table_metadata to construct transactions.
    virtual ss::future<checked<table_metadata, errc>> create_table(
      const table_identifier& table_ident,
      const schema& schema,
      const partition_spec& spec)
      = 0;

    // Gets and returns the resulting table_metadata. Callers are free to use
    // the returned table_metadata to construct transactions.
    virtual ss::future<checked<table_metadata, errc>>
    load_table(const table_identifier& table_ident) = 0;

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
    virtual ss::future<checked<std::nullopt_t, errc>>
    commit_txn(const table_identifier& table_ident, transaction) = 0;
};
std::ostream& operator<<(std::ostream&, catalog::errc);

} // namespace iceberg
