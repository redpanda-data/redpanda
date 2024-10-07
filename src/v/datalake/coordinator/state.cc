/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/state.h"

namespace datalake::coordinator {

std::optional<std::reference_wrapper<const partition_state>>
topics_state::partition_state(const model::topic_partition& tp) const {
    auto state_iter = topic_to_state.find(tp.topic);
    if (state_iter == topic_to_state.end()) {
        return std::nullopt;
    }
    const auto& topic_state = state_iter->second;
    auto prt_iter = topic_state.pid_to_pending_files.find(tp.partition);
    if (prt_iter == topic_state.pid_to_pending_files.end()) {
        return std::nullopt;
    }
    return prt_iter->second;
}

} // namespace datalake::coordinator
