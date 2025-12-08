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
#include "absl/container/flat_hash_map.h"
#include "cluster/fwd.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "test_utils/async.h"
#include "test_utils/test_macros.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <utility>

template<typename Pred>
requires std::predicate<Pred>
void wait_for(model::timeout_clock::duration timeout, Pred&& p) {
    with_timeout(
      model::timeout_clock::now() + timeout,
      ss::do_until(
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

inline auto
get_cloud_storage_configurations(std::string_view hosthame, uint16_t port) {
    net::unresolved_address server_addr(ss::sstring(hosthame), port);
    cloud_storage_clients::s3_configuration s3_conf;
    s3_conf.uri = cloud_storage_clients::access_point_uri(hosthame);
    s3_conf.access_key = cloud_roles::public_key_str("access-key");
    s3_conf.secret_key = cloud_roles::private_key_str("secret-key");
    s3_conf.region = cloud_roles::aws_region_name("us-east-1");
    s3_conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
    s3_conf.server_addr = server_addr;

    archival::configuration a_conf{
      .cloud_storage_initial_backoff = config::mock_binding(100ms),
      .segment_upload_timeout = config::mock_binding(1000ms),
      .manifest_upload_timeout = config::mock_binding(1000ms),
      .garbage_collect_timeout = config::mock_binding(1000ms),
      .upload_loop_initial_backoff = config::mock_binding(100ms),
      .upload_loop_max_backoff = config::mock_binding(5000ms)};
    a_conf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
    a_conf.ntp_metrics_disabled = archival::per_ntp_metrics_disabled::yes;
    a_conf.svc_metrics_disabled = archival::service_metrics_disabled::yes;
    a_conf.time_limit = std::nullopt;

    cloud_storage::configuration c_conf;
    c_conf.client_config = s3_conf;
    c_conf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
    c_conf.connection_limit = archival::connection_limit(2);
    c_conf.cloud_credentials_source
      = model::cloud_credentials_source::config_file;
    return std::make_tuple(
      std::move(s3_conf),
      ss::make_lw_shared<archival::configuration>(std::move(a_conf)),
      c_conf);
}
class cluster_test_fixture {
public:
    using fixture_ptr = std::unique_ptr<redpanda_thread_fixture>;

    cluster_test_fixture()
      : _base_dir(
          "cluster_test."
          + random_generators::with_random_seed().gen_alphanum_string(6)) {
        // Disable all metrics to guard against double_registration errors
        // thrown by seastar. These are simulated nodes which use the same
        // internal metrics implementation, so the usual metrics registration
        // process won't work.
        set_configuration("disable_metrics", true);
        set_configuration("disable_public_metrics", true);
        set_configuration("audit_use_rpc", true);
    }

    virtual ~cluster_test_fixture() {
        std::filesystem::remove_all(std::filesystem::path(_base_dir));
    }

    fixture_ptr make_redpanda_fixture(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      std::optional<int16_t> proxy_port,
      std::optional<int16_t> schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool enable_legacy_upload_mode = true,
      bool iceberg_enabled = false,
      bool cloud_topics_enabled = false,
      bool cluster_linking_enabled = false) {
        return std::make_unique<redpanda_thread_fixture>(
          node_id,
          kafka_port,
          rpc_port,
          proxy_port,
          schema_reg_port,
          seeds,
          ssx::sformat("{}.{}", _base_dir, node_id()),
          false,
          s3_config,
          archival_cfg,
          cloud_cfg,
          use_node_id,
          empty_seed_starts_cluster_val,
          false,
          enable_legacy_upload_mode,
          iceberg_enabled,
          cloud_topics_enabled,
          cluster_linking_enabled);
    }

    void add_node(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      std::optional<int16_t> proxy_port,
      std::optional<int16_t> schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool enable_legacy_upload_mode = true,
      bool iceberg_enabled = false,
      bool cloud_topics_enabled = false,
      bool cluster_linking_enabled = false) {
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
            enable_legacy_upload_mode,
            iceberg_enabled,
            cloud_topics_enabled,
            cluster_linking_enabled));
    }

    application* get_node_application(model::node_id id) {
        return &_instances[id]->app;
    }

    cluster::metadata_cache& get_local_cache(model::node_id id) {
        return _instances[id]->app.metadata_cache.local();
    }

    cluster::id_allocator_frontend&
    get_local_id_allocator_frontend(model::node_id id) {
        return _instances[id]->app.id_allocator_frontend.local();
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
      std::optional<int> proxy_port_base = std::nullopt,
      std::optional<int> schema_reg_port_base = std::nullopt,
      configure_node_id use_node_id = configure_node_id::yes,
      empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool legacy_upload_mode_enabled = true,
      bool iceberg_enabled = false,
      bool cloud_topics_enabled = false,
      bool cluster_linking_enabled = false,
      model::node_id seed_node_id = model::node_id{0}) {
        std::vector<config::seed_server> seeds = {};
        if (!empty_seed_starts_cluster_val || node_id != 0) {
            seeds.push_back(
              {.addr = net::unresolved_address(
                 "127.0.0.1", rpc_port_base + seed_node_id())});
        }
        add_node(
          node_id,
          kafka_port_base + node_id(),
          rpc_port_base + node_id(),
          proxy_port_base.transform(
            [node_id](auto port) { return port + node_id(); }),
          schema_reg_port_base.transform(
            [node_id](auto port) { return port + node_id(); }),
          std::move(seeds),
          use_node_id,
          empty_seed_starts_cluster_val,
          s3_config,
          archival_cfg,
          cloud_cfg,
          legacy_upload_mode_enabled,
          iceberg_enabled,
          cloud_topics_enabled,
          cluster_linking_enabled);
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
          std::nullopt,
          std::nullopt,
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

    ss::future<> create_topic(
      model::topic_namespace_view tp_ns,
      int partitions = 1,
      int16_t replication_factor = 1,
      std::optional<cluster::topic_properties> custom_properties
      = std::nullopt) {
        vassert(!_instances.empty(), "no nodes in the cluster");
        // wait until there is a controller stm leader.
        co_await tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return std::any_of(
              _instances.begin(), _instances.end(), [](auto& it) {
                  return it.second->app.controller->is_raft0_leader();
              });
        });
        auto leader_it = std::find_if(
          _instances.begin(), _instances.end(), [](auto& it) {
              return it.second->app.controller->is_raft0_leader();
          });
        auto& app_0 = leader_it->second->app;
        auto topic_cfg = cluster::topic_configuration{
          tp_ns.ns, tp_ns.tp, partitions, replication_factor};
        if (custom_properties) {
            topic_cfg.properties = custom_properties.value();
        }
        cluster::topic_configuration_vector cfgs = {std::move(topic_cfg)};
        auto results = co_await app_0.controller->get_topics_frontend()
                         .local()
                         .create_topics(
                           cluster::without_custom_assignments(std::move(cfgs)),
                           model::no_timeout);
        RPTEST_REQUIRE_EQ_CORO(results.size(), 1);
        auto& result = results.at(0);
        RPTEST_REQUIRE_EQ_CORO(result.ec, cluster::errc::success);
        auto& leaders = app_0.controller->get_partition_leaders().local();
        co_await tests::cooperative_spin_wait_with_timeout(10s, [&]() {
            auto md = app_0.metadata_cache.local().get_topic_metadata(
              result.tp_ns);
            return md
                   && md->get_assignments().size()
                        == static_cast<size_t>(partitions)
                   && std::all_of(
                     md->get_assignments().begin(),
                     md->get_assignments().end(),
                     [&](const cluster::assignments_set::value_type& p) {
                         return leaders.get_leader(tp_ns, p.second.id);
                     });
        });
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

    ss::future<> shuffle_leadership(model::ntp ntp) {
        RPTEST_REQUIRE_CORO(!_instances.empty());
        auto& app = _instances.begin()->second.get()->app;
        auto& leaders = app.controller->get_partition_leaders().local();
        auto current_leader = leaders.get_leader(ntp);
        if (!current_leader) {
            co_return;
        }
        auto& leader_app = _instances.at(*current_leader).get()->app;
        auto partition = leader_app.partition_manager.local().get(ntp);
        RPTEST_REQUIRE_CORO(partition);
        auto current_leader_id = current_leader.value()();
        auto new_leader_id = model::node_id{
          ++current_leader_id % static_cast<int>(_instances.size())};
        co_return co_await partition
          ->transfer_leadership(
            raft::transfer_leadership_request{
              .group = partition->group(), .target = new_leader_id})
          .discard_result();
    }

    ss::future<> assign_leader(
      model::ntp ntp,
      model::node_id current_leader,
      model::node_id target_node) {
        RPTEST_REQUIRE_CORO(!_instances.empty());
        if (current_leader == target_node) {
            // nothing to be done
            co_return;
        }
        auto& leader_app = _instances.at(current_leader).get()->app;
        auto partition = leader_app.partition_manager.local().get(ntp);
        RPTEST_REQUIRE_CORO(partition);
        co_return co_await partition
          ->transfer_leadership(
            raft::transfer_leadership_request{
              .group = partition->group(), .target = target_node})
          .discard_result();
    }

protected:
    std::vector<model::node_id> instance_ids() const {
        std::vector<model::node_id> ret;
        for (const auto& [id, _] : _instances) {
            ret.push_back(id);
        }
        return ret;
    }

    model::node_id next_node_id() const {
        model::node_id max;
        for (const auto& [id, _] : _instances) {
            max = std::max(max, id);
        }
        if (max < 0) {
            return model::node_id{0};
        }
        return max + model::node_id{1};
    }

    redpanda_thread_fixture* instance(model::node_id id) {
        return _instances[id].get();
    }

private:
    absl::flat_hash_map<model::node_id, fixture_ptr> _instances;

protected:
    ss::sstring _base_dir;
};
