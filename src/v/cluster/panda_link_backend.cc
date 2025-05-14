/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/panda_link_backend.h"

#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/panda_link_table.h"
#include "model/record_batch_types.h"

namespace cluster {
panda_link_backend::panda_link_backend(ss::sharded<panda_link_table>* table)
  : _table(table) {}

bool panda_link_backend::is_batch_applicable(
  const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::panda_link_update;
}

ss::future<std::error_code>
panda_link_backend::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);
    co_await _table->invoke_on_all([&cmd, offset](panda_link_table& table) {
        return ss::visit(
          cmd,
          [&table, offset](panda_link_upsert_cmd update) {
              auto exisiting_id = table.find_id_by_name(update.value.name);
              table.upsert_link(
                exisiting_id.value_or(model::panda_link_id{offset}),
                std::move(update.value));
          },
          [&table](const panda_link_remove_cmd& remove) {
              table.remove_link(remove.key);
          });
    });

    co_return errc::success;
}

ss::future<>
panda_link_backend::fill_snapshot(controller_snapshot& snap) const {
    snap.panda_links.links = _table->local().all_links();
    return ss::now();
}

ss::future<> panda_link_backend::apply_snapshot(
  model::offset, const controller_snapshot& snap) {
    return _table->invoke_on_all(
      [&snap](auto& table) { table.reset_links(snap.panda_links.links); });
}
} // namespace cluster
