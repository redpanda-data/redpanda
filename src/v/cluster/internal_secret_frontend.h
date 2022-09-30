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

#pragma once

#include "cluster/fwd.h"
#include "cluster/internal_secret.h"
#include "cluster/internal_secret_store.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class internal_secret_backend;

// internal_secret_frontend manages internal secrets.
//
// * If this node is the leader, then a secret is looked up based on the key.
//   * If the key doesn't exist, then a password is generated.
// * Else a request is made to the leader
//   * If the remote request was successful, the store is updated.
class internal_secret_frontend
  : public ss::peering_sharded_service<internal_secret_frontend> {
public:
    explicit internal_secret_frontend(
      model::node_id,
      ss::sharded<internal_secret_store>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&);

    ss::future<> start();
    ss::future<> stop();

    ss::future<fetch_internal_secret_reply>
    fetch(internal_secret::key_t key, model::timeout_clock::duration timeout);

private:
    model::node_id _self;
    ss::sharded<internal_secret_store>& _store;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::gate _gate;
};

} // namespace cluster
