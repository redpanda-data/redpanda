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
#include "datalake/coordinator/state_update.h"

#include <seastar/core/future.hh>

namespace datalake::coordinator {

class file_committer {
public:
    enum class errc {
        failed,
        shutting_down,
    };
    virtual ss::future<
      checked<chunked_vector<mark_files_committed_update>, errc>>
    commit_topic_files_to_catalog(model::topic, const topics_state&) const = 0;
    virtual ~file_committer() = default;
};

} // namespace datalake::coordinator
