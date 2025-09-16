/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "container/chunked_vector.h"

namespace cloud_topics::l1 {

// Responsible for issuing `get_compaction_info()` requests to the `metastore`
// when attempting to schedule a round of compactions. This class does not make
// any decisions about whether a `log` needs compacting or not, nor does it
// filter out sampled logs that may not need compaction in its returned
// container from `sample_logs()`. Scheduling decisions such as those are left
// to the other components in the `cloud_topics` compaction subsystem.
class log_sampler {
public:
    log_sampler(metastore*, cluster::metadata_cache*);

    // Populates a vector of `log_info_and_meta` from the provided `log_list_t`
    // by sampling each log's compaction info from the metastore. It is not
    // guaranteed that every log present in `log_list_t` will have an entry in
    // the returned vector, e.g. due to concurrent removal or metastore errors.
    // Also take an optional `size_t` hint for the size of the `log_list_t` (as
    // calling `.size()` on the `intrusive_list` is an O(n)` operation).
    ss::future<chunked_vector<log_info_and_meta>>
    sample_logs(log_list_t&, std::optional<size_t>) const;

private:
    // TODO: owned by `app`.
    metastore* _metastore;

    // Owned by `redpanda` application.
    cluster::metadata_cache* _metadata_cache;
};

} // namespace cloud_topics::l1
