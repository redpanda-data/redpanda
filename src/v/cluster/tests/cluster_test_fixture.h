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
#include "types.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>

#include <utility>

template<typename Pred>
requires std::predicate<Pred>
void wait_for(model::timeout_clock::duration timeout, Pred&& p) {
    with_timeout(
      model::timeout_clock::now() + timeout,
      do_until(
        [p = std::forward<Pred>(p)] { return p(); },
        [] { return ss::sleep(std::chrono::milliseconds(400)); }))
      .get();
}

template<typename T>
void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get();
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

    virtual fixture_ptr make_redpanda_fixture(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      int16_t proxy_port,
      int16_t schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool enable_legacy_upload_mode = true) {
        return std::make_unique<redpanda_thread_fixture>(
          node_id,
          kafka_port,
          rpc_port,
          proxy_port,
          schema_reg_port,
          seeds,
          ssx::sformat("{}.{}", _base_dir, node_id()),
          _sgroups,
          false,
          s3_config,
          archival_cfg,
          cloud_cfg,
          use_node_id,
          empty_seed_starts_cluster_val,
          std::nullopt,
          false,
          enable_legacy_upload_mode);
    }

    void add_node(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      int16_t proxy_port,
      int16_t schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool enable_legacy_upload_mode = true) {
        _instances.emplace(
          node_id,
          make_redpanda_fixture(
            node_id,
            kafka_port,
            rpc_port,
            proxy_port,
            schema_reg_port,
            std::move(seeds),
            use_node_id,
            empty_seed_starts_cluster_val,
            s3_config,
            archival_cfg,
            cloud_cfg,
            enable_legacy_upload_mode));
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
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool legacy_upload_mode_enabled = true) {
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
          std::move(seeds),
          use_node_id,
          empty_seed_starts_cluster_val,
          s3_config,
          archival_cfg,
          cloud_cfg,
          legacy_upload_mode_enabled);
        return get_node_application(node_id);
    }

    application* create_node_application(
      model::node_id node_id,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt) {
        return create_node_application(
          node_id,
          9092,
          11000,
          8082,
          8081,
          use_node_id,
          empty_seed_starts_cluster_val,
          s3_config,
          archival_cfg,
          cloud_cfg);
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
        groups.create_groups().get();
        return groups;
    }

    void create_topic(
      model::topic_namespace_view tp_ns,
      int partitions = 1,
      int16_t replication_factor = 1) {
        vassert(!_instances.empty(), "no nodes in the cluster");
        // wait until there is a controller stm leader.
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return std::any_of(
              _instances.begin(), _instances.end(), [](auto& it) {
                  return it.second->app.controller->linearizable_barrier()
                    .get();
              });
        }).get();
        auto leader_it = std::find_if(
          _instances.begin(), _instances.end(), [](auto& it) {
              return it.second->app.controller->is_raft0_leader();
          });
        auto& app_0 = leader_it->second->app;
        cluster::topic_configuration_vector cfgs = {
          cluster::topic_configuration{
            tp_ns.ns, tp_ns.tp, partitions, replication_factor}};
        auto results = app_0.controller->get_topics_frontend()
                         .local()
                         .create_topics(
                           cluster::without_custom_assignments(std::move(cfgs)),
                           model::no_timeout)
                         .get();
        BOOST_REQUIRE_EQUAL(results.size(), 1);
        auto& result = results.at(0);
        BOOST_REQUIRE_EQUAL(result.ec, cluster::errc::success);
        auto& leaders = app_0.controller->get_partition_leaders().local();
        tests::cooperative_spin_wait_with_timeout(10s, [&]() {
            auto md = app_0.metadata_cache.local().get_topic_metadata(
              result.tp_ns);
            return md && md->get_assignments().size() == partitions
                   && std::all_of(
                     md->get_assignments().begin(),
                     md->get_assignments().end(),
                     [&](const cluster::assignments_set::value_type& p) {
                         return leaders.get_leader(tp_ns, p.second.id);
                     });
        }).get();
    }

    std::tuple<redpanda_thread_fixture*, ss::lw_shared_ptr<cluster::partition>>
    get_leader(const model::ntp& ntp) {
        for (const auto& [_, instance] : _instances) {
            auto& app = instance->app;
            auto p = app.partition_manager.local().get(ntp);
            if (!p) {
                continue;
            }
            if (p->raft()->is_leader()) {
                return std::make_tuple(instance.get(), p);
            }
        }
        return std::make_tuple(nullptr, nullptr);
    }

    void shuffle_leadership(model::ntp ntp) {
        BOOST_REQUIRE(!_instances.empty());
        auto& app = _instances.begin()->second.get()->app;
        auto& leaders = app.controller->get_partition_leaders().local();
        auto current_leader = leaders.get_leader(ntp);
        if (!current_leader) {
            return;
        }
        auto& leader_app = _instances.at(*current_leader).get()->app;
        auto partition = leader_app.partition_manager.local().get(ntp);
        BOOST_REQUIRE(partition);
        partition
          ->transfer_leadership(
            raft::transfer_leadership_request{.group = partition->group()})
          .get();
    }

protected:
    redpanda_thread_fixture* instance(model::node_id id) {
        return _instances[id].get();
    }

    scheduling_groups _sgroups;

private:
    ss::deferred_action<std::function<void()>> _group_deleter;
    absl::flat_hash_map<model::node_id, fixture_ptr> _instances;

protected:
    ss::sstring _base_dir;
};
