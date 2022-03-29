/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/data_policy_table.h"

namespace v8_engine {

bool data_policy_table::insert(
  model::topic_namespace topic, v8_engine::data_policy dp) {
    return _dps.insert({topic, std::move(dp)}).second;
}

bool data_policy_table::erase(model::topic_namespace topic) {
    return _dps.erase(topic) == 1;
}

std::optional<v8_engine::data_policy>
data_policy_table::get_data_policy(const model::topic_namespace& topic) const {
    auto it = _dps.find(topic);
    if (it == _dps.end()) {
        return std::nullopt;
    }
    return it->second;
}

} // namespace v8_engine
