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
#include "cluster/topic_table.h"
#include "container/chunked_vector.h"

namespace cloud_topics::l1 {

class log_sampler {
public:
    log_sampler(metastore*, ss::sharded<cluster::topic_table>*);

    ss::future<chunked_vector<log_info_and_meta>>
    sample_logs(log_list_t&) const;

private:
    metastore* _metastore;
    ss::sharded<cluster::topic_table>* _topic_table;
};

} // namespace cloud_topics::l1
