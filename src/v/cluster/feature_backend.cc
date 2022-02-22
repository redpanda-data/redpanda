/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "feature_backend.h"

#include "seastar/core/coroutine.hh"

namespace cluster {

ss::future<std::error_code>
feature_backend::apply_update(model::record_batch b) {
    std::variant<feature_update_cmd> cmd = co_await cluster::deserialize(
      std::move(b), accepted_commands);

    feature_update_cmd update = std::get<feature_update_cmd>(cmd);
    co_await _feature_table.invoke_on_all(
      [v = update.key.logical_version](feature_table& t) mutable {
          t.set_active_version(v);
      });

    co_return errc::success;
}

} // namespace cluster
