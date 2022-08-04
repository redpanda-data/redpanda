// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "ssx/metrics.h"

#include <absl/container/flat_hash_set.h>

namespace cluster {

class topic_table_probe {
public:
    explicit topic_table_probe(const topic_table&);

    void handle_topic_creation(create_topic_cmd::key_t);
    void handle_topic_deletion(const delete_topic_cmd::key_t&);

private:
    const topic_table& _topic_table;
    absl::flat_hash_map<model::topic_namespace, ss::metrics::metric_groups>
      _topics_metrics;
};

} // namespace cluster
