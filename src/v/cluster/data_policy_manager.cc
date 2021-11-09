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
#include "config/configuration.h"
#include "v8_engine/api.h"
#include "v8_engine/data_policy_table.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <system_error>

namespace cluster {

namespace {

template<typename Cmd>
std::error_code do_apply(
  Cmd cmd,
  v8_engine::data_policy_table& db,
  std::unique_ptr<v8_engine::api>& v8_engine) {
    auto& cfg = config::shard_local_cfg();
    return ss::visit(
      std::move(cmd),
      [&db, &v8_engine, &cfg](create_data_policy_cmd cmd) {
          if (!db.insert(cmd.key, std::move(cmd.value.dp))) {
              return std::error_code(errc::data_policy_already_exists);
          }

          if (cfg.enable_v8() && cfg.developer_mode) {
              auto code = v8_engine->get_code_database_local().get_code(
                cmd.value.dp.script_name());
              if (!code.has_value()) {
                  vlog(
                    clusterlog.error,
                    "Can not get code for data-policy({}) for topic({}). Did "
                    "you publich code by using rpk wasm deploy?",
                    cmd.value.dp,
                    cmd.key);
                  // Need to delete dp from mapping
                  db.erase(cmd.key);
                  return std::error_code(errc::data_policy_js_code_not_exists);
              }

              v8_engine->get_script_dispatcher().insert(
                std::move(cmd.key),
                cmd.value.dp.function_name(),
                std::move(code.value()));
          }

          return std::error_code(errc::success);
      },
      [&db, &v8_engine, &cfg](delete_data_policy_cmd cmd) {
          if (!db.erase(cmd.key)) {
              return std::error_code(errc::data_policy_not_exists);
          }
          if (cfg.enable_v8() && cfg.developer_mode) {
              v8_engine->get_script_dispatcher().remove(std::move(cmd.key));
          }

          return std::error_code(errc::success);
      });
}

template<typename Cmd>
ss::future<std::error_code> dispatch_updates_to_cores(
  Cmd cmd,
  ss::sharded<v8_engine::data_policy_table>& db,
  std::unique_ptr<v8_engine::api>& v8_engine_api) {
    auto res = co_await db.map_reduce0(
      [cmd, &v8_engine_api](v8_engine::data_policy_table& local_db) {
          return do_apply(std::move(cmd), local_db, v8_engine_api);
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
        return dispatch_updates_to_cores(std::move(cmd), _dps, _v8_engine_api);
    });
}

} // namespace cluster
