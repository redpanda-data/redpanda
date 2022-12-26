/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "feature_backend.h"

#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "features/feature_table_snapshot.h"
#include "seastar/core/coroutine.hh"
#include "storage/api.h"

namespace cluster {

ss::future<std::error_code>
feature_backend::apply_update(model::record_batch b) {
    const auto base_offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    if (base_offset <= _feature_table.local().get_applied_offset()) {
        // Special case for systems pre-dating the original_version field: they
        // may have loaded a snapshot that doesn't contain an original version,
        // so must populate it from updates.
        if (
          _feature_table.local().get_original_version()
          == cluster::invalid_version) {
            co_await ss::visit(
              cmd,
              [this](feature_update_cmd update) {
                  return _feature_table.invoke_on_all(
                    [v = update.key.logical_version](
                      features::feature_table& ft) {
                        ft.set_original_version(v);
                    });
              },
              [](feature_update_license_update_cmd) { return ss::now(); });
        }

        co_return errc::success;
    }

    co_await ss::visit(
      cmd,
      [this](feature_update_cmd update) {
          return apply_feature_update_command(std::move(update));
      },
      [this](feature_update_license_update_cmd update) {
          return _feature_table.invoke_on_all(
            [license = std::move(update.key.redpanda_license)](
              features::feature_table& t) mutable {
                t.set_license(std::move(license));
            });
      });

    auto batch_offset = base_offset;
    co_await _feature_table.invoke_on_all(
      [batch_offset](features::feature_table& t) {
          t.set_applied_offset(batch_offset);
      });

    // Updates to the feature table are very infrequent, usually occurring
    // once during an upgrade.  Snapshot on every write, so that as soon
    // as a feature has gone live, we can expect that on subsequent startup
    // it will be live from early in startup (as soon as storage subsystem
    // loads and snapshot can be loaded)
    co_await save_local_snapshot();

    co_return errc::success;
}

ss::future<>
feature_backend::apply_feature_update_command(feature_update_cmd update) {
    co_await _feature_table.invoke_on_all(
      [v = update.key.logical_version](features::feature_table& t) mutable {
          t.set_active_version(v);
      });

    for (const auto& a : update.key.actions) {
        co_await _feature_table.invoke_on_all(
          [a](features::feature_table& t) mutable { t.apply_action(a); });
    }
}

ss::future<> feature_backend::fill_snapshot(controller_snapshot& snap) const {
    snap.features.snap = features::feature_table_snapshot::from(
      _feature_table.local());
    return ss::now();
}

ss::future<> feature_backend::apply_snapshot(
  model::offset, const controller_snapshot& controller_snap) {
    const auto& snap = controller_snap.features.snap;

    auto our_applied_offset = _feature_table.local().get_applied_offset();
    if (our_applied_offset >= snap.applied_offset) {
        vlog(
          clusterlog.debug,
          "skipping applying features state from controller snapshot (applied "
          "offset: {}) as our feature table is already at applied offset {}",
          snap.applied_offset,
          our_applied_offset);
        co_return;
    }

    co_await _feature_table.invoke_on_all(
      [snap](features::feature_table& ft) { snap.apply(ft); });

    // If we didn't already save a snapshot, create it so that subsequent
    // startups see their feature table version immediately, without
    // waiting to apply the controller snapshot.
    if (!has_local_snapshot()) {
        co_await save_local_snapshot();
    }
}

bool feature_backend::has_local_snapshot() {
    return _storage.local()
      .kvs()
      .get(
        storage::kvstore::key_space::controller,
        features::feature_table_snapshot::kvstore_key())
      .has_value();
}

ss::future<> feature_backend::save_local_snapshot() {
    // kvstore is shard-local: must be on a consistent shard every
    // time for snapshot storage to work.
    vassert(ss::this_shard_id() == ss::shard_id{0}, "wrong shard");

    vlog(
      clusterlog.info,
      "Saving feature_table_snapshot at version {}...",
      _feature_table.local().get_active_version());

    auto snapshot = features::feature_table_snapshot::from(
      _feature_table.local());

    auto val_bytes = serde::to_iobuf(snapshot);

    co_await _storage.local().kvs().put(
      storage::kvstore::key_space::controller,
      features::feature_table_snapshot::kvstore_key(),
      std::move(val_bytes));
}

} // namespace cluster
