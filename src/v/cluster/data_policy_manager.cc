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

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <system_error>

namespace cluster {

namespace {

template<typename Cmd>
std::error_code do_apply(Cmd cmd, data_policy_manager::container_type& db) {
    return ss::visit(
      std::move(cmd),
      [&db](create_data_policy_cmd cmd) {
          auto [_, res] = db.insert({cmd.key, std::move(cmd.value.dp)});
          return res ? std::error_code(errc::success)
                     : std::error_code(errc::data_policy_already_exists);
      },
      [&db](delete_data_policy_cmd cmd) {
          if (db.erase(cmd.key) == 0) {
              return std::error_code(errc::data_policy_not_exists);
          } else {
              return std::error_code(errc::success);
          }
      });
}

template<typename Cmd>
ss::future<std::error_code> dispatch_updates_to_cores(
  Cmd cmd, ss::sharded<data_policy_manager::container_type>& db) {
    auto res = co_await db.map_reduce0(
      [cmd](data_policy_manager::container_type& local_db) {
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
