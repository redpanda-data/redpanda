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

#include "cluster/bootstrap_backend.h"

#include "cluster/cluster_uuid.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_backend.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "security/credential_store.h"

namespace cluster {

// node_ids_by_uuid initialization
// ===============================
// If the node UUID to ID map in `members_manager` were not initialized at
// cluster bootstrap, that would have allowed slow seed nodes (ones that miss
// their oportunity to be founding nodes because of being slow) to also possibly
// miss the assignment of `node_id` based on `seed_servers` index (as expected
// for seed nodes). That could happen if another node joins the cluster before a
// slow seed node and grabs the `node_id` that should have belonged to it.
//
// `bootstrap_backend` references `members_manager` to be able to initialize the
// node UUID to ID map in it.

bootstrap_backend::bootstrap_backend(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<storage::api>& storage,
  ss::sharded<members_manager>& members_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<feature_backend>& feature_backend,
  ss::sharded<cluster_recovery_table>& cluster_recovery_table)
  : _credentials(credentials)
  , _storage(storage)
  , _members_manager(members_manager)
  , _feature_table(feature_table)
  , _feature_backend(feature_backend)
  , _cluster_recovery_table(cluster_recovery_table) {}

namespace {

std::error_code
do_apply(user_and_credential cred, security::credential_store& store) {
    if (store.contains(cred.username)) {
        return errc::user_exists;
    }
    store.put(cred.username, std::move(cred.credential));
    return errc::success;
}

template<typename Cmd>
ss::future<std::error_code> dispatch_updates_to_cores(
  Cmd cmd, ss::sharded<security::credential_store>& sharded_service) {
    using res_type = std::optional<std::error_code>;
    const res_type res = co_await sharded_service.map_reduce0(
      [cmd](security::credential_store& service) {
          return do_apply(std::move(cmd), service);
      },
      res_type{},
      [](const res_type result, const std::error_code errc) -> res_type {
          if (!result.has_value()) {
              return errc;
          }
          vassert(
            result.value() == errc,
            "State inconsistency across shards detected, "
            "expected result: {}, have: {}",
            result->value(),
            errc);
          return result;
      });
    co_return res.value();
}

} // namespace

ss::future<std::error_code>
bootstrap_backend::apply_update(model::record_batch b) {
    vlog(clusterlog.info, "Applying update to bootstrap_manager");

    // handle the bootstrap command
    static constexpr auto accepted_commands
      = make_commands_list<bootstrap_cluster_cmd>();
    model::offset offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd, [this, offset](bootstrap_cluster_cmd cmd) {
          return apply(std::move(cmd), offset);
      });
}

ss::future<std::error_code>
bootstrap_backend::apply(bootstrap_cluster_cmd cmd, model::offset offset) {
    // Provide bootstrap_cluster_cmd idempotency
    if (_cluster_uuid_applied) {
        vlog(
          clusterlog.debug,
          "Skipping bootstrap_cluster_cmd {}, current cluster_uuid: {}",
          cmd.value.uuid,
          *_cluster_uuid_applied);
        vassert(
          _storage.local().get_cluster_uuid(),
          "Cluster UUID applied but missing from storage");
        co_return errc::cluster_already_exists;
    }

    // Reconcile with the cluster_uuid value in kvstore
    if (
      _storage.local().get_cluster_uuid()
      && *_storage.local().get_cluster_uuid() != cmd.value.uuid) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Cluster UUID mismatch. Controller log value: {}, kvstore "
          "value: {}. Possible reasons: local controller log storage "
          "wiped while kvstore storage is not, or vice versa",
          cmd.value.uuid,
          *_storage.local().get_cluster_uuid()));
    }

    // Apply bootstrap user
    if (cmd.value.bootstrap_user_cred) {
        const std::error_code errc
          = co_await dispatch_updates_to_cores<user_and_credential>(
            *cmd.value.bootstrap_user_cred, _credentials);
        if (errc == errc::user_exists) {
            vlog(
              clusterlog.warn,
              "Failed to dispatch bootstrap user: {} ({})",
              errc.message(),
              errc);
        } else if (errc) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to dispatch bootstrap user: {} ({})",
              errc.message(),
              errc));
        } else {
            vlog(
              clusterlog.debug,
              "Bootstrap user created {}",
              cmd.value.bootstrap_user_cred->username);
        }
    }

    // Apply initial node UUID to ID map
    co_await _members_manager.local().set_initial_state(
      cmd.value.initial_nodes, cmd.value.node_ids_by_uuid, offset);

    // Apply cluster version to feature table: this activates features without
    // waiting for feature_manager to come up.
    if (cmd.value.founding_version != invalid_version) {
        if (
          _feature_table.local().get_active_version()
          < cmd.value.founding_version) {
            co_await _feature_table.invoke_on_all(
              [v = cmd.value.founding_version](features::feature_table& ft) {
                  ft.bootstrap_active_version(v);
              });
        } else if (
          _feature_table.local().get_original_version()
          < cmd.value.founding_version) {
            // Special case for systems pre-dating the original_version field:
            // they may have loaded a snapshot that doesn't contain an original
            // version, so must populate it from updates.
            co_await _feature_table.invoke_on_all(
              [v = cmd.value.founding_version](features::feature_table& ft) {
                  ft.bootstrap_original_version(v);
              });
        }

        if (_feature_table.local().get_applied_offset() < offset) {
            co_await _feature_table.invoke_on_all(
              [offset](features::feature_table& ft) {
                  ft.set_applied_offset(offset);
              });
        }

        // If we didn't already save a snapshot, create it so that subsequent
        // startups see their feature table version immediately, without
        // waiting to replay the bootstrap message.
        if (!_feature_backend.local().has_local_snapshot()) {
            co_await _feature_backend.local().save_local_snapshot();
        }
    }

    // If this is a recovery cluster, initialize recovery state.
    if (cmd.value.recovery_state.has_value()) {
        co_await _cluster_recovery_table.invoke_on_all(
          [o = offset,
           m = cmd.value.recovery_state->manifest,
           b = cmd.value.recovery_state->bucket](auto& recovery_table) {
              auto ec = recovery_table.apply(o, m, b, wait_for_nodes::yes);
              // We don't expect this since recoveries can only be initialized
              // at or after bootstrap time, but be conservative and handle
              // possible error codes.
              if (ec == errc::update_in_progress) {
                  vlog(
                    clusterlog.error,
                    "Failed to apply recovery state: {} ({})",
                    ec.message(),
                    ec);
              } else if (ec) {
                  throw std::runtime_error(fmt_with_ctx(
                    fmt::format,
                    "Failed to apply recovery state: {} ({})",
                    ec.message(),
                    ec));
              }
          });
    }

    co_await apply_cluster_uuid(cmd.value.uuid);

    co_return errc::success;
}

ss::future<> bootstrap_backend::apply_cluster_uuid(model::cluster_uuid uuid) {
    co_await _storage.invoke_on_all(
      [&new_cluster_uuid = uuid](storage::api& storage) {
          storage.set_cluster_uuid(new_cluster_uuid);
      });
    co_await _storage.local().kvs().put(
      cluster_uuid_key_space,
      bytes::from_string(cluster_uuid_key),
      serde::to_iobuf(uuid));
    _cluster_uuid_applied = uuid;
    vlog(clusterlog.debug, "Cluster UUID initialized {}", uuid);
}

ss::future<> bootstrap_backend::fill_snapshot(controller_snapshot& snap) const {
    snap.bootstrap.cluster_uuid = _cluster_uuid_applied;
    co_return;
}

ss::future<> bootstrap_backend::apply_snapshot(
  model::offset, const controller_snapshot& snap) {
    // when bootstrap_backend processes the bootstrap_cluster_cmd command, it
    // must dispatch updates to other parts of the controller stm
    // (members_manager, feature_table, credential_store). But if we are
    // applying a controller snapshot, these updates will be handled by the
    // sub-stms themselves. Here we only need to initialize the cluster uuid.

    auto snap_cluster_uuid = snap.bootstrap.cluster_uuid;
    vlog(
      clusterlog.debug,
      "cluster uuid from controller snapshot: {}",
      snap_cluster_uuid);

    if (!snap_cluster_uuid) {
        co_return;
    }

    if (
      _storage.local().get_cluster_uuid()
      && *_storage.local().get_cluster_uuid() != snap_cluster_uuid) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Cluster UUID mismatch. Controller snapshot value: {}, kvstore "
          "value: {}. Possible reasons: local controller log storage "
          "wiped while kvstore storage is not, or vice versa",
          snap_cluster_uuid,
          *_storage.local().get_cluster_uuid()));
    }

    if (_cluster_uuid_applied) {
        co_return;
    }

    co_await apply_cluster_uuid(snap_cluster_uuid.value());
}

} // namespace cluster
