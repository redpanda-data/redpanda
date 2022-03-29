/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "v8_engine/data_policy_table.h"

#include <absl/container/flat_hash_map.h>

#include <system_error>

namespace cluster {

class data_policy_manager {
public:
    explicit data_policy_manager(ss::sharded<v8_engine::data_policy_table>& dps)
      : _dps(dps) {}

    static constexpr auto commands
      = make_commands_list<create_data_policy_cmd, delete_data_policy_cmd>();

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type
               == model::record_batch_type::data_policy_management_cmd;
    }

private:
    ss::sharded<v8_engine::data_policy_table>& _dps;
};

} // namespace cluster
