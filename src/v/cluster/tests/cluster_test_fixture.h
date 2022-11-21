/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "cluster/tests/utils.h"
#include "config/seed_server.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "test_utils/async.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>

template<typename Pred>
requires std::predicate<Pred>
void wait_for(model::timeout_clock::duration timeout, Pred&& p) {
    with_timeout(
      model::timeout_clock::now() + timeout,
      do_until(
        [p = std::forward<Pred>(p)] { return p(); },
        [] { return ss::sleep(std::chrono::milliseconds(400)); }))
      .get0();
}

template<typename T>
void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get0();
}

class cluster_test_fixture {
public:
    using fixture_ptr = std::unique_ptr<redpanda_thread_fixture>;

    cluster_test_fixture()
      : _sgroups(create_scheduling_groups())
      , _group_deleter([this] { _sgroups.destroy_groups().get(); })
      , _base_dir("cluster_test." + random_generators::gen_alphanum_string(6)) {
        // Disable all metrics to guard against double_registration errors
        // thrown by seastar. These are simulated nodes which use the same
        // internal metrics implementation, so the usual metrics registration
        // process won't work.
        set_configuration("disable_metrics", true);
        set_configuration("disable_public_metrics", true);
    }

    virtual ~cluster_test_fixture() {
        std::filesystem::remove_all(std::filesystem::path(_base_dir));
    }

    void add_node(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      int16_t proxy_port,
      int16_t schema_reg_port,
      int16_t coproc_supervisor_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes) {
        _instances.emplace(
          node_id,
          std::make_unique<redpanda_thread_fixture>(
            node_id,
            kafka_port,
            rpc_port,
            proxy_port,
            schema_reg_port,
            coproc_supervisor_port,
            seeds,
            ssx::sformat("{}.{}", _base_dir, node_id()),
            _sgroups,
            false,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            use_node_id,
            empty_seed_starts_cluster_val));
    }

    application* get_node_application(model::node_id id) {
        return &_instances[id]->app;
    }

    cluster::metadata_cache& get_local_cache(model::node_id id) {
        return _instances[id]->app.metadata_cache.local();
    }

    ss::sharded<cluster::partition_manager>&
    get_partition_manager(model::node_id id) {
        return _instances[id]->app.partition_manager;
    }

    cluster::shard_table& get_shard_table(model::node_id nid) {
        return _instances[nid]->app.shard_table.local();
    }

    application* create_node_application(
      model::node_id node_id,
      int kafka_port_base = 9092,
      int rpc_port_base = 11000,
      int proxy_port_base = 8082,
      int schema_reg_port_base = 8081,
      int coproc_supervisor_port_base = 43189,
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes) {
        std::vector<config::seed_server> seeds = {};
        if (!empty_seed_starts_cluster_val || node_id != 0) {
            seeds.push_back(
              {.addr = net::unresolved_address("127.0.0.1", 11000)});
        }
        add_node(
          node_id,
          kafka_port_base + node_id(),
          rpc_port_base + node_id(),
          proxy_port_base + node_id(),
          schema_reg_port_base + node_id(),
          coproc_supervisor_port_base + node_id(),
          std::move(seeds),
          use_node_id,
          empty_seed_starts_cluster_val);
        return get_node_application(node_id);
    }

    application* create_node_application(
      model::node_id node_id,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes) {
        return create_node_application(
          node_id,
          9092,
          11000,
          8082,
          8081,
          43189,
          use_node_id,
          empty_seed_starts_cluster_val);
    }

    void remove_node_application(model::node_id node_id) {
        _instances.erase(node_id);
    }

    ss::future<> wait_for_all_members(std::chrono::milliseconds timeout) {
        return tests::cooperative_spin_wait_with_timeout(timeout, [this] {
            return std::all_of(
              _instances.begin(), _instances.end(), [this](auto& p) {
                  return p.second->app.metadata_cache.local().node_count()
                         == _instances.size();
              });
        });
    }

    ss::future<> wait_for_controller_leadership(model::node_id id) {
        return _instances[id]->wait_for_controller_leadership();
    }

    /**
     * Common scheduling groups instance for all nodes, we are limited by
     * max_scheduling_group == 16
     */
    scheduling_groups create_scheduling_groups() {
        scheduling_groups groups;
        groups.create_groups().get0();
        return groups;
    }

private:
    scheduling_groups _sgroups;
    ss::deferred_action<std::function<void()>> _group_deleter;
    absl::flat_hash_map<model::node_id, fixture_ptr> _instances;
    ss::sstring _base_dir;
};
