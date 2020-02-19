#pragma once
#include "cluster/controller.h"
#include "cluster/service.h"
#include "cluster/tests/utils.h"
#include "config/configuration.h"
#include "fmt/format.h"
#include "model/record.h"
#include "raft/service.h"
#include "random/generators.h"
#include "resource_mgmt/memory_groups.h"
#include "rpc/server.h"
#include "rpc/simple_protocol.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "test_utils/async.h"
#include "test_utils/logs.h"
#include "utils/unresolved_address.h"

#include <seastar/net/socket_defs.hh>

using lrk = cluster::log_record_key;
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
        {{.id = model::node_id{1},
          .addr = unresolved_address("127.0.0.1", 9090)}}) {}

    controller_tests_fixture(
      model::node_id node_id,
      int32_t cores,
      int32_t kafka_port,
      int32_t rpc_port,
      std::vector<config::seed_server> seeds)
      : _base_dir("test.dir_" + random_generators::gen_alphanum_string(4))
      , _current_node(
          model::node_id(node_id),
          unresolved_address("127.0.0.1", kafka_port),
          unresolved_address("127.0.0.1", rpc_port),
          std::nullopt,
          model::broker_properties{.cores = ss::smp::count})
      , _seeds(std::move(seeds)) {
        _cli_cache.start().get0();
        _md_cache.start().get0();
        st.start().get0();
        storage::directories::initialize(_base_dir).get0();
    }

    cluster::metadata_cache& get_local_cache() { return _md_cache.local(); }

    cluster::partition_manager& get_local_partition_manger() {
        return _pm.local();
    }

    ~controller_tests_fixture() {
        _rpc.stop().get0();
        if (_controller_started) {
            _controller.stop().get();
        }
        _pm.stop().get0();
        st.stop().get0();
        _md_cache.stop().get0();
        _cli_cache.stop().get0();
    }

    void
    persist_test_batches(ss::circular_buffer<model::record_batch> batches) {
        tests::persist_log_file(
          _base_dir, cluster::controller::ntp, std::move(batches))
          .get0();
    }

    ss::sharded<cluster::controller>& get_controller() {
        config::data_directory_path data_dir_path{
          .path = std::filesystem::path(_base_dir)};
        set_configuration("data_directory", data_dir_path);
        set_configuration("node_id", _current_node.id());
        set_configuration("kafka_api", _current_node.kafka_api_address());
        set_configuration("rpc_server", _current_node.rpc_address());
        set_configuration("seed_servers", _seeds);

        using namespace std::chrono_literals;
        _pm
          .start(
            storage::log_append_config::fsync::yes,
            model::timeout_clock::duration(2s),
            std::ref(st),
            std::ref(_cli_cache))
          .get0();
        _controller
          .start(
            std::ref(_pm),
            std::ref(st),
            std::ref(_md_cache),
            std::ref(_cli_cache))
          .get();
        _controller_started = true;

        rpc::server_configuration rpc_cfg;
        auto rpc_sa = _current_node.rpc_address().resolve().get0();
        rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
        rpc_cfg.addrs.push_back(rpc_sa);
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
                std::ref(_controller));
              s.set_protocol(std::move(proto));
          })
          .get();
        _rpc.invoke_on_all(&rpc::server::start).get();
        return _controller;
    }

    model::ntp make_ntp(const ss::sstring& topic, int32_t partition_id) {
        return model::ntp{
          .ns = _test_ns,
          .tp = {.topic = model::topic(topic),
                 .partition = model::partition_id(partition_id)}};
    }

    ss::circular_buffer<model::record_batch> single_topic_current_broker() {
        ss::circular_buffer<model::record_batch> ret;

        // topic with partition replicas on current broker
        auto b1 = std::move(
                    cluster::simple_batch_builder(
                      cluster::controller::controller_record_batch_type,
                      model::offset(0))
                      .add_kv(
                        lrk{lrk::type::topic_configuration},
                        cluster::topic_configuration(
                          _test_ns, model::topic("topic_1"), 2, 1))
                      // partition 0
                      .add_kv(
                        lrk{lrk::type::partition_assignment},
                        create_test_assignment(
                          "topic_1",
                          0,                         // partition_id
                          {{_current_node.id(), 0}}, // shards_assignment
                          2))                        // group_id
                      // partition 1
                      .add_kv(
                        lrk{lrk::type::partition_assignment},
                        create_test_assignment(
                          "topic_1", 1, {{_current_node.id(), 0}}, 3)))
                    .build();
        ret.push_back(std::move(b1));

        return ret;
    }

    ss::circular_buffer<model::record_batch>
    single_topic_other_broker(model::offset off = model::offset(0)) {
        ss::circular_buffer<model::record_batch> ret;

        // topic with partition replicas on other broker
        auto b1 = std::move(
                    cluster::simple_batch_builder(
                      cluster::controller::controller_record_batch_type, off)
                      .add_kv(
                        lrk{lrk::type::topic_configuration},
                        cluster::topic_configuration(
                          _test_ns, model::topic("topic_2"), 2, 3))
                      // partition 0
                      .add_kv(
                        lrk{lrk::type::partition_assignment},
                        create_test_assignment(
                          "topic_2",
                          0,                        // partition_id
                          {{2, 0}, {3, 0}, {4, 0}}, // shards_assignment
                          5))                       // group_id
                      // partition 1
                      .add_kv(
                        lrk{lrk::type::partition_assignment},
                        create_test_assignment(
                          "topic_2", 1, {{2, 0}, {3, 0}, {4, 0}}, 6)))
                    .build();
        ret.push_back(std::move(b1));

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
        std::array<model::node_id, 5> brokers{model::node_id(0),
                                              model::node_id(1),
                                              model::node_id(2),
                                              model::node_id(3),
                                              model::node_id(4)};
        model::offset offset{0};
        for (int i = 0; i < complex_topic_count; i++) {
            auto partitions = random_generators::get_int(max_partitions);
            auto tp = fmt::format("topic_{}", i);
            cluster::simple_batch_builder builder(
              cluster::controller::controller_record_batch_type, offset);
            builder.add_kv(
              lrk{lrk::type::topic_configuration},
              cluster::topic_configuration(
                _test_ns, model::topic(tp), partitions, 1));
            offset++;

            for (int p = 0; p < partitions; p++) {
                std::vector<std::pair<uint32_t, uint32_t>> replicas;
                replicas.push_back(
                  {_current_node.id(),
                   random_generators::get_int<int16_t>() % ss::smp::count});
                builder.add_kv(
                  lrk{lrk::type::partition_assignment},
                  create_test_assignment(
                    tp,
                    p,                              // partition_id
                    std::move(replicas),            // shards_assignment
                    complex_partitions_count + 1)); // group_id
                offset++;
                complex_partitions_count++;
            }
            ret.push_back(std::move(builder).build());
        }
        return ret;
    }

private:
    static constexpr size_t _max_segment_size = 100'000;
    model::ns _test_ns{"test_ns"};
    std::vector<config::seed_server> _seeds;
    ss::sstring _base_dir;
    model::broker _current_node;
    ss::sharded<rpc::connection_cache> _cli_cache;
    ss::sharded<cluster::metadata_cache> _md_cache;
    ss::sharded<cluster::shard_table> st;
    ss::sharded<cluster::partition_manager> _pm;
    ss::sharded<rpc::server> _rpc;
    bool _controller_started = false;
    ss::sharded<cluster::controller> _controller;
};
// Waits for controller to become a leader it poll every 200ms
void wait_for_leadership(cluster::controller& cntrl) {
    using namespace std::chrono_literals;
    tests::cooperative_spin_wait_with_timeout(10s, [&cntrl] {
        return cntrl.is_leader();
    }).get();
}
