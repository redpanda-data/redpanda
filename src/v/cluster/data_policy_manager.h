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
#include "v8_engine/scripts_dispatcher.h"

#include <system_error>

namespace cluster {

std::error_code
do_apply(set_data_policy_cmd, ss::sharded<v8_engine::scripts_table>& _scripts);

std::error_code do_apply(
  clear_data_policy_cmd, ss::sharded<v8_engine::scripts_table>& _scripts);

class data_policy_manager {
public:
    explicit data_policy_manager(ss::sharded<v8_engine::scripts_table>& scripts)
      : _scripts(scripts) {}

    static constexpr auto commands
      = make_commands_list<set_data_policy_cmd, clear_data_policy_cmd>();

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type
               == model::record_batch_type::data_policy_management_cmd;
    }

private:
    template<typename Cmd, typename Service>
    static ss::future<std::error_code>
    apply(ss::shard_id shard, Cmd cmd, ss::sharded<Service>& service) {
        return service.invoke_on(
          shard, [cmd = std::move(cmd)](auto& local_service) mutable {
              return do_apply(std::move(cmd), local_service);
          });
    }

    ss::sharded<v8_engine::scripts_table>& _scripts;
};

} // namespace cluster
