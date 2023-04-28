// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_has_local_replicas) {
    model::node_id id{2};

    std::vector<model::broker_shard> replicas_1{
      model::broker_shard{model::node_id(1), 2},
      model::broker_shard{model::node_id(2), 1},
      model::broker_shard{model::node_id(3), 0}};

    std::vector<model::broker_shard> replicas_2{
      model::broker_shard{model::node_id(4), 2},
      model::broker_shard{model::node_id(2), 0}, // local replica
    };

    BOOST_REQUIRE_EQUAL(cluster::has_local_replicas(id, replicas_1), false);
    BOOST_REQUIRE_EQUAL(cluster::has_local_replicas(id, replicas_2), true);
}
