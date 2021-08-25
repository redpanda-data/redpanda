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

#include "cluster/data_policy_manager.h"

#include "cluster/cluster_utils.h"

#include <seastar/core/future.hh>

#include <system_error>

namespace cluster {

std::error_code do_apply(
  set_data_policy_cmd cmd, ss::sharded<v8_engine::scripts_table>& _scripts) {
    _scripts.local().update_script(cmd.key, cmd.value.dp);
    return errc::success;
}

std::error_code do_apply(
  clear_data_policy_cmd cmd, ss::sharded<v8_engine::scripts_table>& _scripts) {
    _scripts.local().delete_script(cmd.key);
    return errc::success;
}

ss::future<std::error_code>
data_policy_manager::apply_update(model::record_batch batch) {
    if (_scripts.local_is_initialized()) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    return deserialize(std::move(batch), commands).then([this](auto cmd) {
        return ss::visit(
          std::move(cmd),
          [this](set_data_policy_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _scripts,
                apply<set_data_policy_cmd, v8_engine::scripts_table>);
          },
          [this](clear_data_policy_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _scripts,
                apply<clear_data_policy_cmd, v8_engine::scripts_table>);
          });
    });
}

} // namespace cluster
