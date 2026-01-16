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
#include "container/chunked_vector.h"
#include "iceberg/action.h"
#include "iceberg/manifest_io.h"
#include "iceberg/merge_append_action.h"
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
    explicit transaction(table_metadata table)
      : table_(std::move(table)) {}

    // Sets the current schema, adding it to the table and assigning a new
    // schema id if it doesn't exist. Note, the schema id is ignored, and one
    // is assigned based on the state of the table.
    ss::future<txn_outcome> set_schema(schema);

    // Adds a new partition spec and sets it as the default one. Partition
    // source fields are resolved using the current schema and existing
    // partition fields are reused.
    ss::future<txn_outcome> set_partition_spec(unresolved_partition_spec);

    // Adds the given data files to a new snapshot for the table, potentially
    // merging manifests to reduce manifest list size.
    //
    // If supplied, the given snapshot properties are added to the resulting
    // snapshot.
    //
    // If a tag name is supplied, a snapshot reference "tag" with the given
    // reference expiration will be added to the table with the new snapshot.
    // This will create the tag or replace an existing tag. As long as this tag
    // is not expired, the snapshot is expected to not be removed by snapshot
    // expiration. If no tag expiration is set, snapshot expiration will fall
    // back on table-wide properties.
    ss::future<txn_outcome> merge_append(
      manifest_io&,
      chunked_vector<file_to_append>,
      chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props = {},
      std::optional<ss::sstring> tag_name = std::nullopt,
      std::optional<int64_t> tag_expiration_ms = std::nullopt);

    // Removes expired snapshots from the table, computing expiration based on
    // the given timestamp. Note, this does not perform IO to delete any
    // now-orphaned metadata or data.
    ss::future<txn_outcome> remove_expired_snapshots(model::timestamp now);

    std::optional<action::errc> error() const { return error_; }

    bool is_noop() const {
        return !error_ && updates_.updates.empty()
               && updates_.requirements.empty();
    }

    // NOTE: it is up to the caller to ensure the transaction has not hit any
    // errors before calling these.
    const table_metadata& table() const { return table_; }
    table_metadata release_table() && { return std::move(table_); }
    const updates_and_reqs& updates() const { return updates_; }

    // Construct a transaction object but immediately set error_ to non-nullopt.
    // Only useful for testing.
    static transaction make_with_error(table_metadata table, action::errc err) {
        transaction tx{std::move(table)};
        tx.error_ = err;
        return tx;
    }

    // Release the resulting metadata object. Used for testing.
    table_metadata release_metadata() && { return std::move(table_); }

private:
    // Applies the given action to `table_`.
    ss::future<txn_outcome> apply(std::unique_ptr<action>);

    // Applies the given updates to the table metadata, validating the
    // requirements.
    txn_outcome update_metadata(updates_and_reqs);

    // First error seen by this transaction, if any. If set, no further
    // operations with this transaction will succeed.
    std::optional<action::errc> error_;

    // Table metadata off which to base this transaction.
    table_metadata table_;

    updates_and_reqs updates_;
};

} // namespace iceberg
