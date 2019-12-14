#pragma once

#include "model/record.h"
#include "storage/record_batch_builder.h"
// testing
#include "test_utils/randoms.h"

namespace raft {
struct simple_record_fixture {
    static constexpr int active_nodes = 3;
    template<typename Func>
    model::record_batch_reader reader_gen(std::size_t n, Func&& f) {
        std::vector<model::record_batch> batches;
        batches.reserve(n);
        while (n-- > 0) {
            batches.push_back(f());
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch_reader configs(std::size_t n) {
        return reader_gen(n, [this] { return config_batch(); });
    }
    model::record_batch_reader datas(std::size_t n) {
        return reader_gen(n, [this] { return data_batch(); });
    }
    model::record_batch data_batch() {
        storage::record_batch_builder bldr(raft::data_batch_type, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), rand_iobuf());
        ++_base_offset;
        return std::move(bldr).build();
    }
    model::record_batch config_batch() {
        storage::record_batch_builder bldr(
          raft::configuration_batch_type, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), rpc::serialize(rand_config()));
        ++_base_offset;
        return std::move(bldr).build();
    }
    iobuf rand_iobuf() const {
        iobuf b;
        auto data = random_generators::gen_alphanum_string(100);
        b.append(data.data(), data.size());
        return b;
    }
    raft::group_configuration rand_config() const {
        std::vector<model::broker> nodes;
        std::vector<model::broker> learners;

        for (auto i = 0; i < active_nodes; ++i) {
            nodes.push_back(tests::random_broker(i, i));
            learners.push_back(tests::random_broker(
              active_nodes + 1, active_nodes * active_nodes));
        }
        return raft::group_configuration{
          .leader_id = model::node_id(
            random_generators::get_int(0, active_nodes)),
          .nodes = std::move(nodes),
          .learners = std::move(learners)};
    }
    model::offset _base_offset{0};
    model::ntp _ntp{model::ns(
                      "simple_record_fixture_test_"
                      + random_generators::gen_alphanum_string(8)),
                    model::topic_partition{
                      model::topic(random_generators::gen_alphanum_string(6)),
                      model::partition_id(random_generators::get_int(0, 24))}};
};
} // namespace raft
