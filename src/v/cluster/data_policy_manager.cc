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
#include "cluster/errc.h"
#include "v8_engine/api.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <system_error>

namespace cluster {

namespace {

template<typename Cmd>
std::error_code do_apply(Cmd cmd, v8_engine::api& v8_api) {
    return ss::visit(
      std::move(cmd),
      [&v8_api](const create_data_policy_cmd& cmd) {
          return v8_api.insert_data_policy(cmd.key, cmd.value.dp);
      },
      [&v8_api](const delete_data_policy_cmd& cmd) {
          return v8_api.remove_data_policy(cmd.key);
      });
}

template<typename Cmd>
ss::future<std::error_code>
dispatch_updates_to_cores(Cmd cmd, ss::sharded<v8_engine::api>& v8_api) {
    if (!v8_api.local_is_initialized()) {
        co_return std::error_code(cluster::errc::data_policy_not_enabled);
    }
    auto res = co_await v8_api.map_reduce0(
      [cmd](v8_engine::api& local_v8_api) {
          return do_apply(std::move(cmd), local_v8_api);
      },
      std::optional<std::error_code>{},
      [](std::optional<std::error_code> result, std::error_code error_code) {
          if (!result.has_value()) {
              result = error_code;
          } else {
              vassert(
                result.value() == error_code,
                "State inconsistency across shards detected, "
                "expected result: {}, have: {}",
                result->value(),
                error_code);
          }
          return result.value();
      });
    co_return res.value();
}

} // namespace

ss::future<std::error_code>
data_policy_manager::apply_update(model::record_batch batch) {
    return deserialize(std::move(batch), commands).then([this](auto cmd) {
        return dispatch_updates_to_cores(std::move(cmd), _v8_api);
    });
}

} // namespace cluster
