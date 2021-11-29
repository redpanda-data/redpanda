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
#include "v8_engine/api.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <system_error>

namespace cluster {

namespace {

template<typename Cmd>
ss::future<std::error_code> do_apply(Cmd cmd, v8_engine::api& v8_api) {
    return ss::visit(
      std::move(cmd),
      [&v8_api](create_data_policy_cmd cmd) -> ss::future<std::error_code> {
          return v8_api.insert(cmd.key, std::move(cmd.value.dp));
      },
      [&v8_api](delete_data_policy_cmd cmd) -> ss::future<std::error_code> {
          co_return v8_api.remove(cmd.key);
      });
}

template<typename Cmd>
ss::future<std::error_code>
dispatch_updates_to_cores(Cmd cmd, v8_engine::api& v8_api) {
    auto res = co_await ss::map_reduce(
      boost::irange<unsigned>(0, ss::smp::count),
      [cmd, &v8_api](unsigned core) {
          return ss::smp::submit_to(
            core, [cmd, &v8_api] { return do_apply(std::move(cmd), v8_api); });
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
