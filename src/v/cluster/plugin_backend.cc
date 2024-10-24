/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "plugin_backend.h"

#include "cluster/controller_snapshot.h"

namespace cluster {

plugin_backend::plugin_backend(ss::sharded<plugin_table>* t)
  : _table(t) {}

ss::future<> plugin_backend::fill_snapshot(controller_snapshot& snap) const {
    snap.plugins.transforms = _table->local().all_transforms();
    return ss::now();
}

ss::future<>
plugin_backend::apply_snapshot(model::offset, const controller_snapshot& snap) {
    return _table->invoke_on_all([&snap](auto& table) {
        table.reset_transforms(snap.plugins.transforms);
    });
}

ss::future<std::error_code>
plugin_backend::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
    co_await _table->invoke_on_all([&cmd, offset](plugin_table& table) {
        return ss::visit(
          cmd,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [&table, offset](transform_update_cmd update) {
              // For create operations, the first write's offset is the ID
              auto existing_id = table.find_id_by_name(update.value.name);
              table.upsert_transform(
                existing_id.value_or(model::transform_id(offset())),
                std::move(update.value));
          },
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [&table](const transform_remove_cmd& removal) {
              table.remove_transform(removal.key);
          });
    });
    co_return errc::success;
}

} // namespace cluster
