/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/metadata.h"
#include "v8_engine/data_policy.h"

#include <absl/container/node_hash_map.h>

namespace v8_engine {

class data_policy_table {
public:
    using container_type
      = absl::node_hash_map<model::topic_namespace, v8_engine::data_policy>;

    bool insert(model::topic_namespace, v8_engine::data_policy);

    bool erase(model::topic_namespace);

    std::optional<v8_engine::data_policy>
    get_data_policy(const model::topic_namespace& topic) const;

    size_t size() const { return _dps.size(); }

private:
    container_type _dps;
};

} // namespace v8_engine
