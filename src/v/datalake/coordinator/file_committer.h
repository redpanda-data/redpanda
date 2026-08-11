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
#include "datalake/coordinator/state_update.h"
#include "iceberg/table_identifier.h"

#include <seastar/core/future.hh>

namespace datalake::coordinator {

class file_committer {
public:
    enum class errc {
        failed,
        shutting_down,
    };

    struct commit_result {
        // Updates to replicate to the STM marking the committed files.
        chunked_vector<mark_files_committed_update> updates;

        // Whether the file committer left pending files uncommitted (e.g.
        // there were too many to commit at once). Callers should commit again
        // to continue making progress.
        bool has_more{false};
    };

    virtual ss::future<checked<commit_result, errc>>
    commit_topic_files_to_catalog(model::topic, const topics_state&) const = 0;

    using purge_data = ss::bool_class<struct purge_data_tag>;
    virtual ss::future<checked<std::nullopt_t, errc>>
    drop_table(const iceberg::table_identifier&, purge_data) const = 0;

    virtual ~file_committer() = default;
};

} // namespace datalake::coordinator
