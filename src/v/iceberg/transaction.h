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
#include "container/fragmented_vector.h"
#include "iceberg/action.h"
#include "iceberg/manifest_io.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

#include <seastar/core/future.hh>

namespace iceberg {

// Encapsulates updates to the table metadata. Updates to the table are not
// persisted to the catalog until the transaction is committed, though metadata
// may be written to object storage before then.
//
// This class is not thread safe: it is expected that methods are called one at
// a time.
class transaction {
public:
    using txn_outcome = checked<std::nullopt_t, action::errc>;
    explicit transaction(manifest_io& io, table_metadata table)
      : io_(io)
      , table_(std::move(table)) {}

    // Sets the current schema, adding it to the table and assigning a new
    // schema id if it doesn't exist. Note, the schema id is ignored, and one
    // is assigned based on the state of the table.
    ss::future<txn_outcome> set_schema(schema);
    ss::future<txn_outcome> merge_append(chunked_vector<data_file>);

    std::optional<action::errc> error() const { return error_; }

    // NOTE: it is up to the caller to ensure the transaction has not hit any
    // errors before calling these.
    const table_metadata& table() const { return table_; }
    const updates_and_reqs& updates() const { return updates_; }

private:
    // Applies the given action to `table_`.
    ss::future<txn_outcome> apply(std::unique_ptr<action>);

    // Applies the given updates to the table metadata, validating the
    // requirements.
    txn_outcome update_metadata(updates_and_reqs);

    manifest_io& io_;

    // First error seen by this transaction, if any. If set, no further
    // operations with this transaction will succeed.
    std::optional<action::errc> error_;

    // Table metadata off which to base this transaction.
    table_metadata table_;

    updates_and_reqs updates_;
};

} // namespace iceberg
