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
#include "container/fragmented_vector.h"
#include "datalake/coordinator/file_committer.h"
#include "datalake/coordinator/state_update.h"
#include "iceberg/catalog.h"
#include "iceberg/manifest_io.h"

#include <seastar/core/future.hh>

namespace datalake::coordinator {

class iceberg_file_committer : public file_committer {
public:
    iceberg_file_committer(iceberg::catalog& catalog, iceberg::manifest_io& io)
      : catalog_(catalog)
      , io_(io) {}
    ~iceberg_file_committer() override = default;

    // Commits the given files to the table, creating the table if necessary.
    // Returns updates meant to be replicated to the STM. Until the updates
    // are replicated, the coordinator will not know that the files have been
    // added to the table, and it is expected this call may be repeated, e.g.
    // if leadership changes.
    // XXX: make this method idempotent by deduplicating files!
    //
    // It is up to callers to avoid calling this concurrently for the same
    // table. While this is expected to be safe, concurrent calls will likely
    // result in the calls doing IO to build Iceberg metadata and at least one
    // of the calls failing to commit to the table. Ensuring a single caller
    // avoids this potential waste of IO.
    ss::future<checked<chunked_vector<mark_files_committed_update>, errc>>
    commit_topic_files_to_catalog(
      model::topic, const topics_state&) const final;

private:
    // TODO: pull this out into some helper? Seems useful for other actions.
    iceberg::table_identifier table_id_for_topic(const model::topic& t) const;

    // Loads the table from the catalog, or creates a table with a default
    // schema and default partition spec.
    ss::future<checked<iceberg::table_metadata, errc>>
    load_or_create_table(const iceberg::table_identifier&) const;

    // Must outlive this committer.
    iceberg::catalog& catalog_;
    iceberg::manifest_io& io_;
};

} // namespace datalake::coordinator
