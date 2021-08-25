/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/security_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

security_manager::security_manager(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer)
  : _credentials(credentials)
  , _authorizer(authorizer) {}

ss::future<std::error_code>
security_manager::apply_update(model::record_batch batch) {
    return deserialize(std::move(batch), commands).then([this](auto cmd) {
        return ss::visit(
          std::move(cmd),
          [this](create_user_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _credentials,
                security_manager::
                  apply<create_user_cmd, security::credential_store>);
          },
          [this](delete_user_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _credentials,
                security_manager::
                  apply<delete_user_cmd, security::credential_store>);
          },
          [this](update_user_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _credentials,
                security_manager::
                  apply<update_user_cmd, security::credential_store>);
          },
          [this](create_acls_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _authorizer,
                security_manager::apply<create_acls_cmd, security::authorizer>);
          },
          [this](delete_acls_cmd cmd) {
              return dispatch_updates_to_cores(
                std::move(cmd),
                _authorizer,
                security_manager::apply<delete_acls_cmd, security::authorizer>);
          });
    });
}

std::error_code
do_apply(delete_acls_cmd cmd, security::authorizer& authorizer) {
    authorizer.remove_bindings(cmd.key.filters);
    return errc::success;
}

std::error_code
do_apply(create_acls_cmd cmd, security::authorizer& authorizer) {
    authorizer.add_bindings(cmd.key.bindings);
    return errc::success;
}

std::error_code
do_apply(update_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    if (!removed) {
        return errc::user_does_not_exist;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

std::error_code
do_apply(delete_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    return removed ? errc::success : errc::user_does_not_exist;
}

std::error_code
do_apply(create_user_cmd cmd, security::credential_store& store) {
    if (store.contains(cmd.key)) {
        return errc::user_exists;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

} // namespace cluster
