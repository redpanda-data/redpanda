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
#include "cluster/commands.h"
#include "cluster/controller.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/service.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "fmt/format.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "raft/service.h"
#include "random/generators.h"
#include "resource_mgmt/memory_groups.h"
#include "rpc/dns.h"
#include "rpc/server.h"
#include "rpc/simple_protocol.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "storage/log_manager.h"
#include "test_utils/async.h"
#include "test_utils/logs.h"
#include "utils/unresolved_address.h"

#include <seastar/net/socket_defs.hh>

using namespace std::chrono_literals;

template<typename T>
void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get0();
}
class controller_tests_fixture {
public:
    static constexpr int complex_topic_count{10};
    int complex_partitions_count{0};

    controller_tests_fixture()
      : controller_tests_fixture(
        model::node_id{1},
        ss::smp::count,
        9092,
        9090,
        {},
        "test_dir." + random_generators::gen_alphanum_string(6)) {}

    controller_tests_fixture(
      model::node_id node_id,
      int32_t cores,
      int32_t kafka_port,
      int32_t rpc_port,
      std::vector<config::seed_server> seeds,
      const ss::sstring& base_dir)
      : _base_dir(fmt::format("{}_{}", base_dir, node_id))
      , _current_node(
          model::node_id(node_id),
          unresolved_address("127.0.0.1", kafka_port),
          unresolved_address("127.0.0.1", rpc_port),
          std::nullopt,
          model::broker_properties{.cores = ss::smp::count})
      , _seeds(std::move(seeds)) {
        _cli_cache.start().get0();
        st.start().get0();
        storage::directories::initialize(_base_dir).get0();
    }

    cluster::metadata_cache& get_local_cache() { return _md_cache.local(); }

    cluster::partition_manager& get_local_partition_manger() {
        return _pm.local();
    }

    ss::sharded<cluster::partition_manager>& get_partition_manager() {
        return _pm;
    }

    cluster::shard_table& get_shard_table() { return st.local(); }

    ~controller_tests_fixture() {
        _rpc.stop().get0();
        _metadata_dissemination_service.stop().get0();
        if (_controller_started) {
            _controller->stop().get();
        }
        _pm.stop().get0();
        _gm.stop().get0();
        _storage.stop().get0();
        st.stop().get0();
        _md_cache.stop().get0();
        _cli_cache.stop().get0();
    }

    void
    persist_test_batches(ss::circular_buffer<model::record_batch> batches) {
        tests::persist_log_file(
          _base_dir, model::controller_ntp, std::move(batches))
          .get0();
    }

    cluster::controller* get_controller() {
        config::data_directory_path data_dir_path{
          .path = std::filesystem::path(_base_dir)};
        set_configuration("data_directory", data_dir_path);
        set_configuration("node_id", _current_node.id()());
        set_configuration(
          "kafka_api", _current_node.kafka_advertised_listeners());
        set_configuration("rpc_server", _current_node.rpc_address());
        set_configuration("seed_servers", _seeds);
        set_configuration("disable_metrics", true);
        set_configuration("election_timeout_ms", 500ms);
        set_configuration("raft_heartbeat_interval_ms", 75ms);
        set_configuration("join_retry_timeout_ms", 500ms);

        using namespace std::chrono_literals;
        _storage
          .start(
            storage::kvstore_config(
              1_MiB, 10ms, _base_dir, storage::debug_sanitize_files::yes),
            storage::log_config(
              storage::log_config::storage_type::disk,
              _base_dir,
              1024_MiB,
              storage::debug_sanitize_files::yes))
          .get();
        _storage.invoke_on_all(&storage::api::start).get();
        _gm
          .start(
            model::node_id(config::shard_local_cfg().node_id()),
            model::timeout_clock::duration(2s),
            config::shard_local_cfg().raft_heartbeat_interval_ms(),
            std::ref(_cli_cache),
            std::ref(_storage))
          .get0();
        _gm.invoke_on_all(&raft::group_manager::start).get();
        _pm.start(std::ref(_storage), std::ref(_gm)).get0();
        _controller = std::make_unique<cluster::controller>(
          _cli_cache, _pm, st, _storage);
        _metadata_dissemination_service
          .start(
            std::ref(_gm),
            std::ref(_pm),
            std::ref(_controller->get_partition_leaders()),
            std::ref(_controller->get_members_table()),
            std::ref(_controller->get_topics_state()),
            std::ref(_cli_cache))
          .get0();
        _controller->wire_up().get0();
        _metadata_dissemination_service
          .invoke_on_all(&cluster::metadata_dissemination_service::start)
          .get();

        _md_cache
          .start(
            std::ref(_controller->get_topics_state()),
            std::ref(_controller->get_members_table()),
            std::ref(_controller->get_partition_leaders()))
          .get0();
        _controller_started = true;

        rpc::server_configuration rpc_cfg("cluster_tests_rpc");
        auto rpc_sa = rpc::resolve_dns(_current_node.rpc_address()).get();
        rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
        rpc_cfg.addrs.emplace_back(rpc_sa);
        rpc_cfg.disable_metrics = rpc::metrics_disabled::yes;

        _rpc.start(rpc_cfg).get0();
        _rpc
          .invoke_on_all([this](rpc::server& s) {
              auto proto = std::make_unique<rpc::simple_protocol>();
              proto->register_service<raft::service<
                cluster::partition_manager,
                cluster::shard_table>>(
                ss::default_scheduling_group(),
                ss::default_smp_service_group(),
                _pm,
                st.local());
              proto->register_service<cluster::service>(
                ss::default_scheduling_group(),
                ss::default_smp_service_group(),
                std::ref(_controller->get_topics_frontend()),
                std::ref(_controller->get_members_manager()),
                std::ref(_md_cache));
              proto->register_service<cluster::metadata_dissemination_handler>(
                ss::default_scheduling_group(),
                ss::default_smp_service_group(),
                std::ref(_controller->get_partition_leaders()));
              s.set_protocol(std::move(proto));
          })
          .get();
        _rpc.invoke_on_all(&rpc::server::start).get();
        return _controller.get();
    }

    model::ntp make_ntp(const ss::sstring& topic, int32_t partition_id) {
        return model::ntp(
          test_ns, model::topic(topic), model::partition_id(partition_id));
    }

    cluster::create_topic_cmd make_create_tp_cmd(
      const ss::sstring& tp,
      uint32_t partitions,
      uint16_t rf,
      std::vector<cluster::partition_assignment> assignments) {
        cluster::topic_configuration_assignment cfg(
          cluster::topic_configuration(
            test_ns, model::topic(tp), partitions, rf),
          std::move(assignments));

        return cluster::create_topic_cmd(
          model::topic_namespace(test_ns, model::topic(tp)), std::move(cfg));
    }

    ss::circular_buffer<model::record_batch> single_topic_current_broker() {
        ss::circular_buffer<model::record_batch> ret;
        auto cmd = make_create_tp_cmd(
          "topic_1",
          2,
          1,
          {create_test_assignment(
             "topic_1",
             0,                         // partition_id
             {{_current_node.id(), 0}}, // shards_assignment
             2),                        // group_id
           create_test_assignment("topic_1", 1, {{_current_node.id(), 0}}, 3)});

        ret.push_back(cluster::serialize_cmd(std::move(cmd)).get0());
        return ret;
    }

    ss::circular_buffer<model::record_batch>
    single_topic_other_broker(model::offset off = model::offset(0)) {
        ss::circular_buffer<model::record_batch> ret;
        auto cmd = make_create_tp_cmd(
          "topic_1",
          2,
          1,
          {create_test_assignment(
             "topic_2",
             0,                        // partition_id
             {{2, 0}, {3, 0}, {4, 0}}, // shards_assignment
             5),                       // group_id
           create_test_assignment("topic_2", 1, {{2, 0}, {3, 0}, {4, 0}}, 6)});

        ret.push_back(cluster::serialize_cmd(std::move(cmd)).get0());
        return ret;
    }

    ss::circular_buffer<model::record_batch> two_topics() {
        ss::circular_buffer<model::record_batch> ret;
        auto first = single_topic_current_broker();
        auto second = single_topic_other_broker(model::offset(3));
        std::move(first.begin(), first.end(), std::back_inserter(ret));
        std::move(second.begin(), second.end(), std::back_inserter(ret));
        return ret;
    }

    ss::circular_buffer<model::record_batch> make_complex_topics() {
        ss::circular_buffer<model::record_batch> ret;

        auto max_partitions = 20;
        std::array<model::node_id, 5> brokers{
          model::node_id(0),
          model::node_id(1),
          model::node_id(2),
          model::node_id(3),
          model::node_id(4)};
        model::offset offset{0};
        for (int i = 0; i < complex_topic_count; i++) {
            auto partitions = random_generators::get_int(max_partitions);
            auto tp = fmt::format("topic_{}", i);
            auto cmd = make_create_tp_cmd(tp, partitions, 1, {});

            offset++;
            for (int p = 0; p < partitions; p++) {
                std::vector<std::pair<uint32_t, uint32_t>> replicas;
                replicas.emplace_back(
                  _current_node.id(),
                  random_generators::get_int<int16_t>() % ss::smp::count);

                cmd.value.assignments.push_back(create_test_assignment(
                  tp,
                  p,                              // partition_id
                  std::move(replicas),            // shards_assignment
                  complex_partitions_count + 1)); // group_id

                offset++;
                complex_partitions_count++;
            }
            ret.push_back(cluster::serialize_cmd(std::move(cmd)).get0());
        }
        return ret;
    }

private:
    static constexpr size_t _max_segment_size = 100'000;
    std::vector<config::seed_server> _seeds;
    ss::sstring _base_dir;
    model::broker _current_node;
    ss::sharded<rpc::connection_cache> _cli_cache;
    ss::sharded<cluster::metadata_cache> _md_cache;
    ss::sharded<cluster::shard_table> st;
    ss::sharded<storage::api> _storage;
    ss::sharded<raft::group_manager> _gm;
    ss::sharded<cluster::partition_manager> _pm;
    ss::sharded<rpc::server> _rpc;
    bool _controller_started = false;
    ss::sharded<cluster::metadata_dissemination_service>
      _metadata_dissemination_service;
    ss::sharded<storage::kvstore> _kvstore;
    std::unique_ptr<cluster::controller> _controller;
};
// Waits for controller to become a leader it poll every 200ms
static void wait_for_leadership(cluster::partition_leaders_table& leaders) {
    using namespace std::chrono_literals;

    leaders
      .wait_for_leader(model::controller_ntp, ss::lowres_clock::now() + 10s, {})
      .get0();
}
