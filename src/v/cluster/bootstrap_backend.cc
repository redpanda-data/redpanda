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
#include "cluster/logger.h"
#include "cluster/types.h"
#include "security/credential_store.h"

namespace cluster {

bootstrap_backend::bootstrap_backend(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<storage::api>& storage)
  : _credentials(credentials)
  , _storage(storage) {}

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
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd,
      ss::coroutine::lambda(
        [this](bootstrap_cluster_cmd cmd) -> ss::future<std::error_code> {
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

            // Apply cluster_uuid
            co_await _storage.invoke_on_all(
              [&new_cluster_uuid = cmd.value.uuid](storage::api& storage) {
                  storage.set_cluster_uuid(new_cluster_uuid);
                  return ss::make_ready_future();
              });
            co_await _storage.local().kvs().put(
              cluster_uuid_key_space,
              cluster_uuid_key,
              serde::to_iobuf(cmd.value.uuid));
            _cluster_uuid_applied = cmd.value.uuid;
            vlog(
              clusterlog.debug, "Cluster UUID initialized {}", cmd.value.uuid);

            co_return errc::success;
        }));
}

} // namespace cluster
