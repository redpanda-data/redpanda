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
#include "iceberg/manifest_io.h"
#include "iceberg/table_metadata.h"

#include <seastar/core/future.hh>

namespace iceberg {

/**
 * Enum class allowing changing an expected type returned from query
 */
enum class result_type { snapshot, manifest_file, manifest };

template<typename ElemT>
using matcher = absl::FunctionRef<bool(const ElemT&)>;
/**
 * Metadata query representing data structure. Query result_type template
 * parameter controls what information should be returned after query is
 * executed. The matchers are predicates returning `true` if an entry should be
 * included in the result. Empty matcher matches all entities. Matchers are
 * applied one after another i.e first the snapshot if it matches, the manifest
 * file matcher and finally the manifest matcher. The matchers are related with
 * AND relationship i.e. if snapshot matcher is set the snapshot containing a
 * manifest_file must match. Returned result is an outcome of depth first search
 * traversal of metadata tree.
 *
 * Deduplication:
 *
 * Results are deduplicated i.e. if two or more snapshots include the same
 * manifest file, only one of them will be included in the result, the same rule
 * applies to the manifest. Deduplication is based on the path as it is a unique
 * identifier of metadata entity.
 *
 * Ordering:
 *
 * The order in which results are collected matches the order of DFS metadata
 * tree traversal with deduplication.
 */
template<result_type ResultT>
struct metadata_query {
    static constexpr result_type r_type{ResultT};

    std::optional<matcher<snapshot>> snapshot_matcher;
    std::optional<matcher<manifest_file>> manifest_file_matcher;
    std::optional<matcher<manifest>> manifest_matcher;
};

/**
 * Class encapsulating metadata query execution logic, an executor can execute
 * query over provided table metadata and it is using provided manifest_io to
 * retrieve information if required.
 */
class metadata_query_executor {
public:
    enum class errc {
        metadata_io_error,
        table_metadata_inconsistency,
    };
    explicit metadata_query_executor(
      manifest_io& io, const table_metadata& table)
      : io_(&io)
      , table_(&table) {}

    ss::future<checked<chunked_vector<snapshot>, errc>>
    execute_query(const metadata_query<result_type::snapshot>&) const;

    ss::future<checked<chunked_vector<manifest_file>, errc>>
    execute_query(const metadata_query<result_type::manifest_file>&) const;

    ss::future<checked<chunked_vector<manifest>, errc>>
    execute_query(const metadata_query<result_type::manifest>&) const;

private:
    manifest_io* io_;

    // Table metadata which the query will be executed against
    const table_metadata* table_;
};

} // namespace iceberg
