// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/ephemeral_credential_frontend.h"

#include "cluster/ephemeral_credential_rpc_service.h"
#include "cluster/ephemeral_credential_serde.h"
#include "cluster/logger.h"
#include "features/feature_table.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential.h"
#include "security/ephemeral_credential_store.h"
#include "security/exceptions.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "vformat.h"

#include <seastar/coroutine/exception.hh>

#include <fmt/ranges.h>

// namespace

namespace cluster {

namespace {

constexpr size_t credential_length{32};
constexpr auto timeout = 5s;

using scram = security::scram_sha512_authenticator::auth::scram;

security::ephemeral_credential
make_ephemeral_credential(security::acl_principal const& principal) {
    constexpr auto gen = []() {
        return random_generators::gen_alphanum_string(credential_length);
    };
    return {security::ephemeral_credential{
      principal,
      security::credential_user{gen()},
      security::credential_password{gen()},
      security::scram_sha512_authenticator::name}};
}

security::scram_credential
make_scram_credential(security::ephemeral_credential const& cred) {
    return scram::make_credentials(
      cred.principal(), cred.password(), scram::min_iterations);
}

} // namespace

using namespace std::chrono_literals;

ephemeral_credential_frontend::ephemeral_credential_frontend(
  model::node_id self,
  ss::sharded<security::credential_store>& c_store,
  ss::sharded<security::ephemeral_credential_store>& e_store,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<rpc::connection_cache>& connections)
  : _self(self)
  , _c_store{c_store}
  , _e_store{e_store}
  , _feature_table{feature_table}
  , _connections{connections}
  , _gate{} {}

ss::future<ephemeral_credential_frontend::get_return>
ephemeral_credential_frontend::get(security::acl_principal const& principal) {
    auto guard = _gate.hold();
    get_return res;

    if (!_feature_table.local().is_active(
          features::feature::ephemeral_secrets)) {
        vlog(
          clusterlog.info,
          "Ephemeral credentials feature is not active (upgrade in progress?)");
        res.err = errc::invalid_node_operation;
        co_return res;
    }

    if (auto it = _e_store.local().find(principal); !_e_store.local().has(it)) {
        res.credential = make_ephemeral_credential(principal);
        co_await put(res.credential);
    } else {
        res.credential = *it;
    }

    co_return res;
}

ss::future<> ephemeral_credential_frontend::put(
  security::acl_principal principal,
  security::credential_user user,
  security::scram_credential cred) {
    auto guard = _gate.hold();
    // Add the principal to the supplied scram_credential
    cred = {
      cred.salt(),
      cred.server_key(),
      cred.stored_key(),
      cred.iterations(),
      std::move(principal)};
    co_await _c_store.invoke_on_all(
      [user{std::move(user)}, cred{std::move(cred)}](auto& store) {
          store.put(user, cred);
      });
}

ss::future<>
ephemeral_credential_frontend::put(security::ephemeral_credential cred) {
    auto guard = _gate.hold();
    co_await _e_store.invoke_on_all(
      [cred](auto& store) { store.insert_or_assign(cred); });
}

ss::future<std::error_code> ephemeral_credential_frontend::inform(
  model::node_id node_id, security::acl_principal const& principal) {
    auto guard = _gate.hold();

    auto e_cred_res = co_await get(principal);
    auto err = e_cred_res.err;
    if (err) {
        co_return err;
    }

    if (_self == node_id) {
        vlog(clusterlog.debug, "Inform self: {}", e_cred_res.credential);
        co_await put(
          e_cred_res.credential.principal(),
          e_cred_res.credential.user(),
          make_scram_credential(e_cred_res.credential));
        co_return errc::success;
    }

    vlog(clusterlog.debug, "Inform {}: {}", node_id, e_cred_res.credential);
    auto req = put_ephemeral_credential_request{
      principal,
      e_cred_res.credential.user(),
      make_scram_credential(e_cred_res.credential)};
    auto res = rpc::get_ctx_data<put_ephemeral_credential_reply>(
      co_await _connections.local()
        .with_node_client<impl::ephemeral_credential_client_protocol>(
          _self,
          ss::this_shard_id(),
          node_id,
          timeout,
          [req{std::move(req)}](
            impl::ephemeral_credential_client_protocol proto) mutable {
              return proto.put_ephemeral_credential(
                std::move(req), rpc::client_opts{timeout});
          }));

    if (res.has_value()) {
        err = res.assume_value().err;
    } else if (res.has_error()) {
        err = res.assume_error();
    }
    if (err) {
        vlog(
          clusterlog.info,
          "Failed to inform node: {} of ephemeral_credential: {}, error: {}",
          node_id,
          e_cred_res.credential,
          err);
    }
    co_return err;
}

ss::future<> ephemeral_credential_frontend::start() { return ss::now(); }
ss::future<> ephemeral_credential_frontend::stop() { return _gate.close(); }

} // namespace cluster
