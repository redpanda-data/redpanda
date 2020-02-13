#include "model/fundamental.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/models.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/randoms.h"
#include "utils/copy_range.h"
// testing
#include "test_utils/fixture.h"

#include <seastar/core/future-util.hh>

struct foreign_entry_fixture {
    static constexpr int active_nodes = 3;
    foreign_entry_fixture()
      : _mngr(storage::log_config{
        .base_dir = ".",
        .max_segment_size = 1 << 30,
        .should_sanitize = storage::log_config::sanitize_files::yes}) {
        (void)_mngr.manage(_ntp).get0();
    }

    std::vector<storage::append_result> write_n(const std::size_t n) {
        auto cfg = storage::log_append_config{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};
        std::vector<storage::append_result> res;
        res.push_back(gen_data_record_batch_reader(n)
                        .consume(get_log().make_appender(cfg), cfg.timeout)
                        .get0());
        res.push_back(gen_config_record_batch_reader(n)
                        .consume(get_log().make_appender(cfg), cfg.timeout)
                        .get0());
        get_log().flush().get();
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
        storage::record_batch_builder bldr(raft::data_batch_type, _base_offset);
        bldr.add_raw_kv(rand_iobuf(), rand_iobuf());
        ++_base_offset;
        return std::move(bldr).build();
    }
    model::record_batch config_batch() {
        storage::record_batch_builder bldr(
          raft::configuration_batch_type, _base_offset);
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
            nodes.push_back(tests::random_broker(i, i));
            learners.push_back(tests::random_broker(
              active_nodes + 1, active_nodes * active_nodes));
        }
        return raft::group_configuration{.nodes = std::move(nodes),
                                         .learners = std::move(learners)};
    }
    ~foreign_entry_fixture() { _mngr.stop().get(); }
    model::offset _base_offset{0};
    storage::log get_log() { return _mngr.get(_ntp).value(); }
    storage::log_manager _mngr;
    model::ntp _ntp{
      model::ns("bootstrap_test_" + random_generators::gen_alphanum_string(8)),
      model::topic_partition{
        model::topic(random_generators::gen_alphanum_string(6)),
        model::partition_id(random_generators::get_int(0, 24))}};
};

FIXTURE_TEST(sharing_one_reader, foreign_entry_fixture) {
    std::vector<model::record_batch_reader> copies =
      // clang-format off
      raft::details::foreign_share_n(gen_config_record_batch_reader(3),
        ss::smp::count).get0();
    // clang-format on

    BOOST_REQUIRE_EQUAL(copies.size(), ss::smp::count);
    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        info("Submitting shared reader to shard:{}", shard);
        auto cfg =
          // MUST return the config; otherwise thread exception
          ss::smp::submit_to(
            shard,
            [e = std::move(copies[shard])]() mutable {
                info("extracting configuration");
                return raft::details::extract_configuration(std::move(e));
            })
            .get0()
            .value();

        for (auto& n : cfg.nodes) {
            BOOST_REQUIRE(
              n.id() >= 0 && n.id() <= foreign_entry_fixture::active_nodes);
        }
        for (auto& n : cfg.learners) {
            BOOST_REQUIRE(n.id() > foreign_entry_fixture::active_nodes);
        }
    }
}

FIXTURE_TEST(sharing_correcteness_test, foreign_entry_fixture) {
    auto copies = 5;
    auto batches = storage::test::make_random_batches(
      model::offset(0), 50, true);
    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    auto refs = raft::details::share_n(std::move(rdr), 2).get0();
    auto shared = raft::details::foreign_share_n(
                    std::move(refs.back()), ss::smp::count)
                    .get0();
    refs.pop_back();
    auto reference_batches = model::consume_reader_to_memory(
                               std::move(refs.back()), model::no_timeout)
                               .get0();

    BOOST_REQUIRE_EQUAL(shared.size(), ss::smp::count);
    for (auto& copy : shared) {
        auto shared = model::consume_reader_to_memory(
                        std::move(copy), model::no_timeout)
                        .get0();
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
                         .get0();
    }
    BOOST_REQUIRE_EQUAL(share_copies.size(), ss::smp::count);

    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        info("Submitting shared raft::entry to shard:{}", shard);
        auto cfg = ss::smp::submit_to(
                     shard,
                     [es = std::move(share_copies[shard])]() mutable {
                         return ss::do_with(
                           std::move(es), [](model::record_batch_reader& es) {
                               return raft::details::extract_configuration(
                                 std::move(es));
                           });
                     })
                     .get0()
                     .value();

        for (auto& n : cfg.nodes) {
            BOOST_REQUIRE(
              n.id() >= 0 && n.id() <= foreign_entry_fixture::active_nodes);
        }
        for (auto& n : cfg.learners) {
            BOOST_REQUIRE(n.id() > foreign_entry_fixture::active_nodes);
        }
    }
}
