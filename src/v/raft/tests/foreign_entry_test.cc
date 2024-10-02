// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "test_utils/randoms.h"
#include "utils/copy_range.h"
// testing
#include "test_utils/fixture.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>

#include <boost/test/tools/old/interface.hpp>

#include <optional>

using namespace std::chrono_literals; // NOLINT

struct foreign_entry_fixture {
    static constexpr int active_nodes = 3;
    ss::sstring test_dir = "test.data."
                           + random_generators::gen_alphanum_string(10);

    foreign_entry_fixture()
      : _storage(
          [this]() {
              return storage::kvstore_config(
                1_MiB,
                config::mock_binding(10ms),
                test_dir,
                storage::make_sanitized_file_config());
          },
          [this]() {
              return storage::log_config(
                test_dir,
                1_GiB,
                ss::default_priority_class(),
                storage::make_sanitized_file_config());
          },
          _feature_table) {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        _storage.start().get();
        (void)_storage.log_mgr()
          .manage(storage::ntp_config(_ntp, "test.dir"))
          .get();
    }

    std::vector<storage::append_result> write_n(const std::size_t n) {
        auto cfg = storage::log_append_config{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};
        std::vector<storage::append_result> res;
        res.push_back(
          gen_data_record_batch_reader(n)
            .for_each_ref(get_log()->make_appender(cfg), cfg.timeout)
            .get());
        res.push_back(
          gen_config_record_batch_reader(n)
            .for_each_ref(get_log()->make_appender(cfg), cfg.timeout)
            .get());
        get_log()->flush().get();
        return res;
    }
    template<typename Func>
    model::record_batch_reader reader_gen(std::size_t n, Func&& f) {
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(n);
        while (n-- > 0) {
            batches.push_back(f());
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch_reader gen_config_record_batch_reader(std::size_t n) {
        return reader_gen(n, [this] { return config_batch(); });
    }
    model::record_batch_reader gen_data_record_batch_reader(std::size_t n) {
        return reader_gen(n, [this] { return data_batch(); });
    }
    model::record_batch data_batch() {
        storage::record_batch_builder bldr(
          model::record_batch_type::raft_data, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), rand_iobuf());
        ++_base_offset;
        return std::move(bldr).build();
    }
    model::record_batch config_batch() {
        storage::record_batch_builder bldr(
          model::record_batch_type::raft_configuration, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), reflection::to_iobuf(rand_config()));
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
            nodes.push_back(model::random_broker(i, i));
            learners.push_back(model::random_broker(
              active_nodes + 1, active_nodes * active_nodes));
        }
        return raft::group_configuration(
          std::move(nodes), model::revision_id(1));
    }
    ~foreign_entry_fixture() {
        _storage.stop().get();
        _feature_table.stop().get();
    }
    model::offset _base_offset{0};
    ss::logger _test_logger{"foreign-test-logger"};
    ss::sharded<features::feature_table> _feature_table;
    storage::api _storage;
    ss::shared_ptr<storage::log> get_log() {
        return _storage.log_mgr().get(_ntp);
    }
    model::ntp _ntp{
      model::ns("test.bootstrap." + random_generators::gen_alphanum_string(8)),
      model::topic(random_generators::gen_alphanum_string(6)),
      model::partition_id(random_generators::get_int(0, 24))};
};

ss::future<raft::group_configuration>
extract_configuration(model::record_batch_reader&& rdr) {
    struct noop_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch&) {
            co_return ss::stop_iteration::no;
        }
        int end_of_stream() { return 0; }
    };

    auto [_, cfgs] = co_await raft::details::for_each_ref_extract_configuration(
      model::offset(0), std::move(rdr), noop_consumer{}, model::no_timeout);
    BOOST_REQUIRE(!cfgs.empty());
    co_return cfgs.begin()->cfg;
}

FIXTURE_TEST(sharing_one_reader, foreign_entry_fixture) {
    std::vector<model::record_batch_reader> copies =
      // clang-format off
      raft::details::foreign_share_n(gen_config_record_batch_reader(3),
        ss::smp::count).get();
    // clang-format on

    BOOST_REQUIRE_EQUAL(copies.size(), ss::smp::count);
    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        info("Submitting shared reader to shard:{}", shard);
        auto cfg =
          // MUST return the config; otherwise thread exception
          ss::smp::submit_to(shard, [e = std::move(copies[shard])]() mutable {
              info("extracting configuration");
              return extract_configuration(std::move(e));
          }).get();

        for (auto& rni : cfg.current_config().voters) {
            BOOST_REQUIRE(
              rni.id() >= 0 && rni.id() <= foreign_entry_fixture::active_nodes);
        }
        for (auto& rni : cfg.current_config().learners) {
            BOOST_REQUIRE(rni.id() > foreign_entry_fixture::active_nodes);
        }
    }
}

FIXTURE_TEST(sharing_correcteness_test, foreign_entry_fixture) {
    auto batches
      = model::test::make_random_batches(model::offset(0), 50, true).get();
    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    auto refs = raft::details::share_n(std::move(rdr), 2).get();
    auto shared = raft::details::foreign_share_n(
                    std::move(refs.back()), ss::smp::count)
                    .get();
    refs.pop_back();
    auto reference_batches = model::consume_reader_to_memory(
                               std::move(refs.back()), model::no_timeout)
                               .get();

    BOOST_REQUIRE_EQUAL(shared.size(), ss::smp::count);
    for (auto& copy : shared) {
        auto shared = model::consume_reader_to_memory(
                        std::move(copy), model::no_timeout)
                        .get();
        for (int i = 0; i < reference_batches.size(); ++i) {
            BOOST_REQUIRE_EQUAL(shared[i], reference_batches[i]);
        }
    }
}

FIXTURE_TEST(copy_lots_of_readers, foreign_entry_fixture) {
    std::vector<model::record_batch_reader> share_copies;
    {
        auto rdr = gen_config_record_batch_reader(1);
        share_copies = raft::details::foreign_share_n(
                         std::move(rdr), ss::smp::count)
                         .get();
    }
    BOOST_REQUIRE_EQUAL(share_copies.size(), ss::smp::count);

    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        info("Submitting shared raft::entry to shard:{}", shard);
        auto cfg = ss::smp::submit_to(
                     shard,
                     [es = std::move(share_copies[shard])]() mutable {
                         return ss::do_with(
                           std::move(es), [](model::record_batch_reader& es) {
                               return extract_configuration(std::move(es));
                           });
                     })
                     .get();

        cfg.for_each_voter([](const raft::vnode& rni) {
            BOOST_REQUIRE(
              rni.id() >= 0 && rni.id() <= foreign_entry_fixture::active_nodes);
        });

        cfg.for_each_learner([](const raft::vnode& rni) {
            BOOST_REQUIRE(rni.id() > foreign_entry_fixture::active_nodes);
        });
    }
}
