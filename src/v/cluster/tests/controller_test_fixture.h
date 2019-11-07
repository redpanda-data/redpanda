#pragma once
#include "cluster/controller.h"
#include "cluster/tests/utils.h"
#include "fmt/format.h"
#include "model/record.h"
#include "random/generators.h"
#include "storage/directories.h"
#include "test_utils/logs.h"

using lrk = cluster::log_record_key;

class controller_tests_fixture {
public:
    static constexpr int complex_topic_count{100};
    int complex_partitions_count{0};
    controller_tests_fixture()
      : _base_dir("test_dir_" + random_generators::gen_alphanum_string(4)) {
        _cli_cache.start().get0();
        _md_cache.start().get0();
        st.start().get0();
        using namespace std::chrono_literals;
        storage::directories::initialize(_base_dir).get0();
        _pm
          .start(
            _current_node,
            10s,
            _base_dir,
            _max_segment_size,
            storage::log_append_config::fsync::no,
            model::timeout_clock::duration(10s),
            std::ref(st),
            std::ref(_cli_cache))
          .get0();
    }

    cluster::metadata_cache& get_local_cache() {
        return _md_cache.local();
    }

    ~controller_tests_fixture() {
        st.stop().get0();
        _md_cache.stop().get0();
        _cli_cache.stop().get0();
        _pm.stop().get0();
    }

    void persist_test_batches(std::vector<model::record_batch> batches) {
        tests::persist_log_file(
          _base_dir, cluster::controller::ntp, std::move(batches))
          .get0();
    }

    cluster::controller get_controller() {
        return cluster::controller(
          _current_node,
          _base_dir,
          _max_segment_size,
          std::ref(_pm),
          std::ref(st),
          std::ref(_md_cache));
    }

    model::ntp make_ntp(const sstring& topic, int32_t partition_id) {
        return model::ntp{
          .ns = _test_ns,
          .tp = {.topic = model::topic(topic),
                 .partition = model::partition_id(partition_id)}};
    }

    std::vector<model::record_batch> single_topic_current_broker() {
        std::vector<model::record_batch> ret;

        // topic with partition replicas on current broker
        auto b1
          = std::move(
              cluster::simple_batch_builder(
                cluster::controller::controller_record_batch_type,
                model::offset(0))
                .add_kv(
                  lrk{lrk::type::topic_configuration},
                  cluster::topic_configuration(
                    _test_ns, model::topic("topic_1"), 2, 3))
                // partition 0
                .add_kv(
                  lrk{lrk::type::partition_assignment},
                  create_test_assignment(
                    "topic_1",
                    0,                                      // partition_id
                    {{_current_node(), 0}, {2, 0}, {3, 0}}, // shards_assignment
                    2))                                     // group_id
                // partition 1
                .add_kv(
                  lrk{lrk::type::partition_assignment},
                  create_test_assignment(
                    "topic_1", 1, {{_current_node(), 0}, {2, 0}, {3, 0}}, 3)))
              .build();
        ret.push_back(std::move(b1));

        return ret;
    }

    std::vector<model::record_batch>
    single_topic_other_broker(model::offset off = model::offset(0)) {
        std::vector<model::record_batch> ret;

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

    std::vector<model::record_batch> two_topics() {
        std::vector<model::record_batch> ret;
        auto first = single_topic_current_broker();
        auto second = single_topic_other_broker(model::offset(3));
        std::move(first.begin(), first.end(), std::back_inserter(ret));
        std::move(second.begin(), second.end(), std::back_inserter(ret));
        return ret;
    }

    std::vector<model::record_batch> make_complex_topics() {
        std::vector<model::record_batch> ret;

        auto max_partitions = 20;
        auto max_rf = 5;
        std::array<model::node_id, 5> brokers{model::node_id(0),
                                              model::node_id(1),
                                              model::node_id(2),
                                              model::node_id(3),
                                              model::node_id(4)};
        model::offset offset{0};
        for (int i = 0; i < complex_topic_count; i++) {
            auto partitions = random_generators::get_int(max_partitions);
            auto rf = random_generators::get_int(max_rf);
            auto tp = fmt::format("topic_{}", i);
            cluster::simple_batch_builder builder(
              cluster::controller::controller_record_batch_type, offset);
            builder.add_kv(
              lrk{lrk::type::topic_configuration},
              cluster::topic_configuration(
                _test_ns, model::topic(tp), partitions, rf));
            offset++;

            for (int p = 0; p < partitions; p++) {
                std::vector<std::pair<uint32_t, uint32_t>> replicas;
                replicas.reserve(rf);
                for (int r = 0; r < rf; r++) {
                    replicas.push_back({brokers[r](), 0});
                }
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
    static constexpr model::node_id _current_node{1};

    model::ns _test_ns{"test_ns"};
    sstring _base_dir;
    sharded<raft::client_cache> _cli_cache;
    sharded<cluster::metadata_cache> _md_cache;
    sharded<cluster::shard_table> st;
    sharded<cluster::partition_manager> _pm;
};