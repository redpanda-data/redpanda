/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"

#include <absl/container/flat_hash_map.h>

#include <system_error>

namespace cluster {

class data_policy_manager {
public:
    using container_type
      = absl::flat_hash_map<model::topic_namespace, v8_engine::data_policy>;

    static constexpr auto commands
      = make_commands_list<create_data_policy_cmd, delete_data_policy_cmd>();

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type
               == model::record_batch_type::data_policy_management_cmd;
    }

    const container_type& get_current_state() const { return _dps.local(); }

    ss::future<> start() { return _dps.start(); }
    ss::future<> stop() { return _dps.stop(); }

private:
    ss::sharded<container_type> _dps;
};

} // namespace cluster
