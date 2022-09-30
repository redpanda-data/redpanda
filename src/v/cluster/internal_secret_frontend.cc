// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/internal_secret_frontend.h"

#include "cluster/internal_secret.h"
#include "cluster/internal_secret_service_impl.h"
#include "cluster/internal_secret_store.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "features/feature_table.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "vformat.h"

namespace {

constexpr size_t password_len = 128;

cluster::internal_secret::value_t generate_password() {
    return cluster::internal_secret::value_t{
      random_generators::get_bytes(password_len)};
}

} // namespace

namespace cluster {
using namespace std::chrono_literals;

internal_secret_frontend::internal_secret_frontend(
  model::node_id self,
  ss::sharded<internal_secret_store>& store,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders)
  : _self{self}
  , _store{store}
  , _feature_table{feature_table}
  , _connections{connections}
  , _leaders{leaders}
  , _gate{} {}

ss::future<fetch_internal_secret_reply> internal_secret_frontend::fetch(
  internal_secret::key_t key, model::timeout_clock::duration timeout) {
    auto guard = _gate.hold();

    if (!_feature_table.local().is_active(
          features::feature::internal_secrets)) {
        vlog(
          clusterlog.info,
          "Internal secrets feature is not active (upgrade in progress?)");
        co_return fetch_internal_secret_reply{{}, errc::invalid_node_operation};
    }

    auto leader_opt = _leaders.local().get_leader(model::controller_ntp);

    if (!leader_opt.has_value()) {
        auto ec = errc::no_leader_controller;
        vlog(clusterlog.info, "{}", ec);
        co_return fetch_internal_secret_reply{{}, ec};
    }

    if (leader_opt == _self) {
        internal_secret secret{key, {}};
        auto val = _store.local().get(key);
        if (!val) {
            val = generate_password();
            secret = {key, *val};
            co_await _store.invoke_on_all(
              [secret](auto& store) { store.insert_or_assign(secret); });
        }
        co_return fetch_internal_secret_reply{secret, errc::success};
    }

    auto ctx
      = co_await _connections.local()
          .with_node_client<impl::internal_secret_client_protocol>(
            _self,
            ss::this_shard_id(),
            leader_opt.value(),
            timeout,
            [key, timeout](impl::internal_secret_client_protocol proto) {
                return proto.fetch({timeout, key}, rpc::client_opts{timeout});
            });

    auto res = rpc::get_ctx_data<fetch_internal_secret_reply>(std::move(ctx));

    if (res.has_value()) {
        auto reply = std::move(res).assume_value();
        if (reply.ec == errc::success) {
            co_await _store.invoke_on_all([secret{reply.secret}](auto& store) {
                store.insert_or_assign(secret);
            });
        }
        co_return reply;
    }
    co_return fetch_internal_secret_reply{internal_secret{}, errc::timeout};
}

ss::future<> internal_secret_frontend::start() { return ss::now(); }
ss::future<> internal_secret_frontend::stop() { co_await _gate.close(); }

} // namespace cluster
