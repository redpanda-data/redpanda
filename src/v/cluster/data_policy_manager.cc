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

#include "cluster/data_policy_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/errc.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <system_error>

namespace cluster {

namespace {

template<typename Cmd>
std::error_code do_apply(Cmd cmd, v8_engine::data_policy_table& db) {
    return ss::visit(
      std::move(cmd),
      [&db](create_data_policy_cmd cmd) {
          return db.insert(cmd.key, std::move(cmd.value.dp))
                   ? std::error_code(errc::success)
                   : std::error_code(errc::data_policy_already_exists);
      },
      [&db](delete_data_policy_cmd cmd) {
          return db.erase(cmd.key)
                   ? std::error_code(errc::success)
                   : std::error_code(errc::data_policy_not_exists);
      });
}

template<typename Cmd>
ss::future<std::error_code> dispatch_updates_to_cores(
  Cmd cmd, ss::sharded<v8_engine::data_policy_table>& db) {
    auto res = co_await db.map_reduce0(
      [cmd](v8_engine::data_policy_table& local_db) {
          return do_apply(std::move(cmd), local_db);
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
        return dispatch_updates_to_cores(std::move(cmd), _dps);
    });
}

} // namespace cluster
