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

#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/randoms.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
// testing
#include "test_utils/randoms.h"

namespace raft {
struct simple_record_fixture {
    static constexpr int active_nodes = 3;
    template<typename Func>
    model::record_batch_reader reader_gen(std::size_t n, Func&& f) {
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(n);
        while (n-- > 0) {
            batches.push_back(f());
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch_reader
    configs(std::size_t n, raft::group_configuration::version_t version) {
        return reader_gen(n, [this, version] { return config_batch(version); });
    }

    model::record_batch_reader datas(std::size_t n) {
        return reader_gen(n, [this] { return data_batch(); });
    }
    model::record_batch data_batch() {
        storage::record_batch_builder bldr(
          model::record_batch_type::raft_data, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), rand_iobuf());
        ++_base_offset;
        return std::move(bldr).build();
    }
    model::record_batch
    config_batch(raft::group_configuration::version_t version) {
        auto batches = details::serialize_configuration_as_batches(
          rand_config(version));
        return std::move(batches.front());
    }

    iobuf rand_iobuf() const {
        iobuf b;
        auto data = random_generators::gen_alphanum_string(100);
        b.append(data.data(), data.size());
        return b;
    }
    raft::group_configuration
    rand_config(raft::group_configuration::version_t version) const {
        std::vector<model::broker> nodes;
        std::vector<raft::vnode> voters;
        std::vector<raft::vnode> learners;

        for (auto i = 0; i < active_nodes; ++i) {
            nodes.push_back(model::random_broker(0, 1000));
        }

        if (version < raft::group_configuration::v_5) {
            return raft::group_configuration(
              std::move(nodes), model::revision_id{});
        } else {
            std::vector<raft::vnode> voters;

            for (auto i = 0; i < active_nodes; ++i) {
                voters.emplace_back(
                  tests::random_named_int<model::node_id>(),
                  tests::random_named_int<model::revision_id>());
            }

            raft::group_configuration cfg(
              voters, tests::random_named_int<model::revision_id>());

            cfg.set_version(version);
            return cfg;
        }
    }

    model::offset _base_offset{0};
    model::ntp _ntp{
      ss::sstring("simple_record_fixture_test_")
        + random_generators::gen_alphanum_string(8),
      random_generators::gen_alphanum_string(6),
      random_generators::get_int(0, 24)};
};
} // namespace raft
