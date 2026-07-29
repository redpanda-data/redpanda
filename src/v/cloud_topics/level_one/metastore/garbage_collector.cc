/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/garbage_collector.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/metastore/simple_stm.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"

namespace cloud_topics::l1 {

garbage_collector::garbage_collector(simple_stm* stm, io* io)
  : stm_(stm)
  , io_(io) {}

ss::future<std::expected<void, garbage_collector::error>>
garbage_collector::remove_unreferenced_objects(ss::abort_source* as) {
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return std::unexpected(error{"sync error"});
    }
    // TODO: once cloud recovery is implemented, base the decisions of what to
    // remove on the state that has been persisted to cloud.
    const auto& s = stm_->state();

    chunked_vector<object_location> to_remove;
    for (const auto& [oid, obj_entry] : s.objects) {
        if (obj_entry.is_preregistration) {
            continue;
        }
        if (obj_entry.total_data_size != obj_entry.removed_data_size) {
            continue;
        }

        // TODO: split these into multiple updates in case we've got a lot of
        // objects to remove.
        to_remove.push_back(
          object_location{
            .id = oid,
            .ts_path = obj_entry.imported_ts_location.transform(
              [](const imported_ts_object_location& loc) {
                  return loc.ts_path;
              })});
        vlog(cd_log.debug, "Deleting L1 object: {}", oid);
    }
    if (to_remove.empty()) {
        co_return std::expected<void, error>{};
    }
    co_return co_await remove_objects(std::move(to_remove), as);
}

ss::future<std::expected<void, garbage_collector::error>>
garbage_collector::remove_objects(
  chunked_vector<object_location> to_remove, ss::abort_source* as) {
    if (to_remove.empty()) {
        co_return std::expected<void, error>{};
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return std::unexpected(error{"sync error"});
    }
    auto del_res = co_await io_->delete_objects(to_remove.copy(), as);
    if (!del_res.has_value()) {
        co_return std::unexpected(error{"io error"});
    }
    chunked_vector<object_id> remove_ids;
    remove_ids.reserve(to_remove.size());
    for (const auto& ext : to_remove) {
        remove_ids.emplace_back(ext.id);
    }
    auto update_res = remove_objects_update::build(
      stm_->state(), std::move(remove_ids));
    if (!update_res.has_value()) {
        co_return std::unexpected(error{"logic error"});
    }
    storage::record_batch_builder builder(
      model::record_batch_type::l1_stm, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(remove_objects_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), *as);
    if (!repl_res.has_value()) {
        co_return std::unexpected(error{"replication error"});
    }
    // NOTE: no explicit post-replication validation, just because correctness
    // here isn't absolutely critical, so we don't bother checking all the
    // objects.
    co_return std::expected<void, error>{};
}

} // namespace cloud_topics::l1
