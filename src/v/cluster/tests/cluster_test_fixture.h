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

#pragma once
#include "cluster/controller.h"
#include "cluster/partition_manager.h"
#include "cluster/tests/controller_test_fixture.h"
#include "config/seed_server.h"
#include "model/metadata.h"
#include "random/generators.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

template<typename Pred>
CONCEPT(requires std::predicate<Pred>)
void wait_for(model::timeout_clock::duration timeout, Pred&& p) {
    with_timeout(
      model::timeout_clock::now() + timeout,
      do_until(
        [p = std::forward<Pred>(p)] { return p(); },
        [] { return ss::sleep(std::chrono::milliseconds(400)); }))
      .get0();
}

class cluster_test_fixture {
public:
    using fixture_ptr = std::unique_ptr<controller_tests_fixture>;

    cluster_test_fixture()
      : _base_dir("cluster_test." + random_generators::gen_alphanum_string(6)) {
        set_configuration("disable_metrics", true);
    }

    void add_controller(
      model::node_id node_id,
      uint32_t cores,
      int16_t kafka_port,
      int16_t rpc_port,
      std::vector<config::seed_server> seeds) {
        _instances.emplace(
          node_id,
          std::make_unique<controller_tests_fixture>(
            node_id, cores, kafka_port, rpc_port, seeds, _base_dir));
    }

    cluster::controller* get_controller(model::node_id id) {
        return _instances[id]->get_controller();
    }

    cluster::metadata_cache& get_local_cache(model::node_id id) {
        return _instances[id]->get_local_cache();
    }

    ss::sharded<cluster::partition_manager>&
    get_partition_manager(model::node_id id) {
        return _instances.find(id)->second->get_partition_manager();
    }

    cluster::shard_table& get_shard_table(model::node_id nid) {
        return _instances.find(nid)->second->get_shard_table();
    }

    cluster::controller* create_controller(
      model::node_id node_id, int kafka_port = 9092, int rpc_port = 11000) {
        std::vector<config::seed_server> seeds = {};
        if (node_id != 0) {
            seeds.push_back({.addr = unresolved_address("127.0.0.1", 11000)});
        }
        add_controller(
          node_id,
          ss::smp::count,
          kafka_port + node_id(),
          rpc_port + node_id(),
          std::move(seeds));

        return get_controller(node_id);
    }

    void remove_controller(model::node_id node_id) {
        _instances.erase(node_id);
    }

private:
    absl::flat_hash_map<model::node_id, fixture_ptr> _instances;
    ss::sstring _base_dir;
};
