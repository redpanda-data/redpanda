/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/api.h"

#include "cluster/errc.h"

namespace v8_engine {
ss::future<> api::insert_code(coproc::script_id id, iobuf code) {
    co_await _executor.insert_or_assign(id, std::move(code));
}

ss::future<> api::erase_code(coproc::script_id id) {
    co_await _executor.erase(id);
}

std::error_code api::insert_data_policy(
  const model::topic_namespace& topic, const data_policy& dp) {
    if (!_dp_table.insert(topic, dp)) {
        return std::error_code(cluster::errc::data_policy_already_exists);
    }
    return std::error_code(cluster::errc::success);
}

std::error_code api::remove_data_policy(const model::topic_namespace& topic) {
    if (!_dp_table.erase(topic)) {
        return std::error_code(cluster::errc::data_policy_not_exists);
    }

    _script_dispatcher.remove(topic);

    return std::error_code(cluster::errc::success);
}

std::optional<data_policy>
api::get_data_policy(const model::topic_namespace& topic) {
    return _dp_table.get_data_policy(topic);
}

} // namespace v8_engine
