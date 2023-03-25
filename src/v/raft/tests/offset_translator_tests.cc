// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "raft/offset_translator.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/fwd.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals; // NOLINT

static model::record_batch create_batch(
  model::record_batch_type type,
  model::offset o,
  size_t length = 1,
  size_t data_size = 0) {
    storage::record_batch_builder b(type, o);
    for (size_t i = 0; i < length; ++i) {
        iobuf value;
        if (data_size > 0) {
            value = random_generators::make_iobuf(data_size);
        }
        b.add_raw_kv(iobuf{}, std::move(value));
    }
    return std::move(b).build();
}

struct base_fixture {
    base_fixture()
      : _test_dir(
        fmt::format("test_{}", random_generators::gen_alphanum_string(6))) {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        _api
          .start(
            [this]() { return make_kv_cfg(); },
            [this]() { return make_log_cfg(); },
            std::ref(_feature_table))
          .get();
        _api.invoke_on_all(&storage::api::start).get();
    }

    storage::kvstore_config make_kv_cfg() const {
        return storage::kvstore_config(
          1_MiB,
          config::mock_binding(10ms),
          _test_dir,
          storage::debug_sanitize_files::yes);
    }

    storage::log_config make_log_cfg() const {
        return storage::log_config(
          _test_dir, 100_MiB, storage::debug_sanitize_files::yes);
    }

    raft::offset_translator make_offset_translator() {
        return raft::offset_translator{
          {model::record_batch_type::raft_configuration,
           model::record_batch_type::checkpoint},
          raft::group_id(0),
          test_ntp,
          _api.local()};
    }

    model::ntp test_ntp = model::ntp(
      model::ns("test"), model::topic("tp"), model::partition_id(0));
    ss::sstring _test_dir;
    ss::sharded<features::feature_table> _feature_table;
    ss::sharded<storage::api> _api;

    ~base_fixture() {
        _api.stop().get();
        _feature_table.stop().get();
    }
};

void validate_translation(
  raft::offset_translator& tr,
  model::offset log_offset,
  model::offset kafka_offset) {
    BOOST_REQUIRE_EQUAL(tr.state()->from_log_offset(log_offset), kafka_offset);
    BOOST_REQUIRE_EQUAL(tr.state()->to_log_offset(kafka_offset), log_offset);
}

struct offset_translator_fixture : base_fixture {
    offset_translator_fixture()
      : tr(make_offset_translator()) {
        tr.start(raft::offset_translator::must_reset::yes, {}).get();
    }

    void validate_offset_translation(
      model::offset log_offset, model::offset kafka_offset) {
        validate_translation(tr, log_offset, kafka_offset);
    }

    raft::offset_translator tr;
};

FIXTURE_TEST(test_translating_to_kafka_offsets, offset_translator_fixture) {
    std::set<model::offset> batch_offsets{
      model::offset(0),
      model::offset(1),
      // data batch @ 2 -> kafka 0
      model::offset(3),
      model::offset(4),
      model::offset(5),
      // data batch @ 6 -> kafka 1
      model::offset(7),
      model::offset(8),
      // data batch @ 9 -> kafka 3
      model::offset(10)};

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    validate_offset_translation(model::offset(2), model::offset(0));
    validate_offset_translation(model::offset(6), model::offset(1));
    validate_offset_translation(model::offset(9), model::offset(2));
    validate_offset_translation(model::offset(11), model::offset(3));
    validate_offset_translation(model::offset(12), model::offset(4));
}

FIXTURE_TEST(
  test_translating_to_kafka_offsets_first, offset_translator_fixture) {
    std::set<model::offset> batch_offsets{// data batch @ 0 -> kafka 0
                                          // data batch @ 1 -> kafka 1
                                          model::offset(2),
                                          model::offset(3),
                                          model::offset(4),
                                          model::offset(5),
                                          model::offset(6),
                                          // data batch @ 7 -> kafka 2
                                          model::offset(8),
                                          // data batch @ 9 -> kafka 3
                                          model::offset(10)};

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    validate_offset_translation(model::offset(0), model::offset(0));
    validate_offset_translation(model::offset(1), model::offset(1));
    validate_offset_translation(model::offset(7), model::offset(2));
    validate_offset_translation(model::offset(9), model::offset(3));
    validate_offset_translation(model::offset(11), model::offset(4));
}

FIXTURE_TEST(random_translation_test, offset_translator_fixture) {
    auto batches_count = 1000;
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    // go over whole offset space
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.state()->from_log_offset(log_offset);
        auto reverse_log_offset = tr.state()->to_log_offset(kafka_offset);
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(random_translation_test_with_hint, offset_translator_fixture) {
    auto batches_count = 1000;
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    // go over whole offset space
    model::offset prev_log_offset;
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.state()->from_log_offset(log_offset);
        auto reverse_log_offset = tr.state()->to_log_offset(
          kafka_offset, prev_log_offset);
        prev_log_offset = reverse_log_offset;
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(immutability_test, offset_translator_fixture) {
    auto batches_count = 100;
    auto end_offset = 1100; // exclusive
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(1000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    tr.process(create_batch(
      model::record_batch_type::raft_data, model::offset(end_offset - 1)));

    // log offset -> kafka offset
    std::unordered_map<model::offset, model::offset> offsets_mapping;

    // go over whole offset space
    for (auto o : boost::irange(0, end_offset)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.state()->from_log_offset(log_offset);
        auto reverse_log_offset = tr.state()->to_log_offset(kafka_offset);

        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
        offsets_mapping.emplace(log_offset, kafka_offset);
    }

    auto validate_offsets_immutable = [&](int64_t start) {
        for (auto o : boost::irange<int64_t>(start, end_offset)) {
            model::offset log_offset(o);
            if (batch_offsets.contains(log_offset)) {
                continue;
            }
            model::offset k_offset = tr.state()->from_log_offset(log_offset);
            // validate that offset havent changed
            BOOST_REQUIRE_EQUAL(offsets_mapping[log_offset], k_offset);
        }
    };

    // prefix truncate
    auto truncate_at = random_generators::get_int(end_offset - 1);
    tr.prefix_truncate(model::offset(truncate_at)).get();

    validate_offsets_immutable(truncate_at + 1);

    // add new config at the end
    tr.process(create_batch(
      model::record_batch_type::raft_configuration,
      model::offset(end_offset + 1)));

    validate_offsets_immutable(truncate_at + 1);
}

static ss::future<std::vector<model::offset>>
collect_base_offsets(storage::log log) {
    struct consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch& batch) {
            res.push_back(batch.base_offset());
            co_return ss::stop_iteration::no;
        }

        std::vector<model::offset> end_of_stream() { return std::move(res); }

        std::vector<model::offset> res;
    };

    auto r = co_await log.make_reader(storage::log_reader_config(
      log.offsets().start_offset,
      log.offsets().dirty_offset,
      ss::default_priority_class()));
    co_return co_await r.for_each_ref(consumer{}, model::no_timeout);
}

struct fuzz_checker {
    fuzz_checker(
      storage::log log,
      std::function<raft::offset_translator()>&& make_offset_translator)
      : _make_offset_translator(std::move(make_offset_translator))
      , _log(std::move(log)) {}

    ss::future<> start() {
        _tr.emplace(_make_offset_translator());
        co_await _tr->start(raft::offset_translator::must_reset::yes, {});
        co_await _tr->sync_with_log(_log, std::nullopt);
    }

    ss::future<> append() {
        size_t number_of_batches = random_generators::get_int(1, 5);
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(number_of_batches);

        for (size_t i_batch = 0; i_batch < number_of_batches; ++i_batch) {
            auto batch_type = all_batch_types[random_generators::get_int(
              all_batch_types.size() - 1)];

            size_t batch_length = random_generators::get_int(1, 3);
            auto batch = create_batch(
              batch_type, model::offset{0}, batch_length, 512_KiB);
            batches.push_back(std::move(batch));
        }

        class consumer {
        public:
            consumer(fuzz_checker& self)
              : _self(self)
              , _appender(self._log.make_appender(storage::log_append_config{
                  .should_fsync = storage::log_append_config::fsync::no,
                  .io_priority = ss::default_priority_class(),
                  .timeout = model::no_timeout})) {}

            ss::future<ss::stop_iteration>
            operator()(model::record_batch& batch) {
                auto ret = co_await _appender(batch);

                if (_self._tr) {
                    _self._tr->process(batch);
                }

                for (auto o = batch.base_offset(); o <= batch.last_offset();
                     ++o) {
                    _self._kafka_offsets.push_back(
                      model::offset{_self._log_offsets.size()});
                    if (
                      batch.header().type
                      == model::record_batch_type::raft_data) {
                        _self._log_offsets.push_back(o);
                    }
                }

                co_return ret;
            }

            auto end_of_stream() { return _appender.end_of_stream(); }

        private:
            fuzz_checker& _self;
            storage::log_appender _appender;
        };

        co_await model::make_memory_record_batch_reader(std::move(batches))
          .for_each_ref(consumer(*this), model::no_timeout);

        if (_tr) {
            (void)ss::with_gate(
              _gate, [this] { return _tr->maybe_checkpoint(2_MiB); });
        }
    }

    ss::future<> truncate() {
        if (!_tr) {
            // If we truncate with offset translator offline, it is possible
            // that its state will later diverge from the log (because
            // highest_known_offset will become bogus). So we forbid this
            // scenario in tests. In client code this means that we should
            // truncate log only if offset translator was truncated
            // successfully.
            co_return;
        }

        auto batch_base_offsets = co_await collect_base_offsets(_log);
        if (batch_base_offsets.empty()) {
            co_return;
        }

        model::offset truncate_at
          = batch_base_offsets[random_generators::get_int(
            batch_base_offsets.size() - 1)];

        co_await _tr->truncate(truncate_at);

        co_await _log.truncate(
          storage::truncate_config(truncate_at, ss::default_priority_class()));

        if (_log.offsets().dirty_offset() < 0) {
            _kafka_offsets.clear();
            _log_offsets.clear();
        } else {
            _kafka_offsets.resize(_log.offsets().dirty_offset() + 1);
            while (!_log_offsets.empty()
                   && _log_offsets.back()() >= _kafka_offsets.size()) {
                _log_offsets.pop_back();
            }
        }
    }

    ss::future<> prefix_truncate() {
        auto batch_base_offsets = co_await collect_base_offsets(_log);
        if (batch_base_offsets.empty()) {
            co_return;
        }

        model::offset new_start_offset
          = batch_base_offsets[random_generators::get_int(
            batch_base_offsets.size() - 1)];

        co_await _log.truncate_prefix(storage::truncate_prefix_config(
          new_start_offset, ss::default_priority_class()));
        BOOST_REQUIRE_EQUAL(new_start_offset, _log.offsets().start_offset);

        _snapshot_offset = new_start_offset;
        if (_snapshot_offset() > 0) {
            --_snapshot_offset;
        } else {
            _snapshot_offset = model::offset{};
        }

        if (_tr) {
            co_await _tr->prefix_truncate(_snapshot_offset);
        }

        if (new_start_offset() < _kafka_offsets.size()) {
            _snapshot_delta = new_start_offset
                              - _kafka_offsets[new_start_offset];
        } else {
            _snapshot_delta = _kafka_offsets.size() - _log_offsets.size();
        }

        // _kafka_offsets and _log_offsets don't get prefix-truncated, we
        // rely on _snapshot_offset to filter out prefix-truncated entries.
    }

    ss::future<> stop() {
        if (_tr) {
            co_await _gate.close();
            _tr.reset();
        }
    }

    ss::future<> restart() {
        if (!_tr) {
            _tr.emplace(_make_offset_translator());
            _gate = ss::gate{};

            co_await _tr->start(raft::offset_translator::must_reset::no, {});
            co_await _tr->prefix_truncate_reset(
              _snapshot_offset, _snapshot_delta);
            co_await _tr->sync_with_log(_log, std::nullopt);
        }
    }

    void validate() {
        if (!_tr) {
            return;
        }

        // check translation for high watermark (first unoccupied offset)
        model::offset hwm_lo{_kafka_offsets.size()};
        model::offset hwm_ko{_log_offsets.size()};
        BOOST_TEST_CONTEXT("With log offset: " << hwm_lo) {
            BOOST_REQUIRE_EQUAL(hwm_ko, _tr->state()->from_log_offset(hwm_lo));
        }
        BOOST_TEST_CONTEXT("With kafka offset: " << hwm_ko) {
            BOOST_REQUIRE_EQUAL(hwm_lo, _tr->state()->to_log_offset(hwm_ko));
        }

        int64_t start_log_offset = model::next_offset(_snapshot_offset)();
        if (start_log_offset >= _kafka_offsets.size()) {
            // empty log
            return;
        }

        for (int64_t lo = start_log_offset; lo < _kafka_offsets.size(); ++lo) {
            BOOST_TEST_CONTEXT("With log offset: " << lo) {
                BOOST_REQUIRE_EQUAL(
                  _kafka_offsets[lo],
                  _tr->state()->from_log_offset(model::offset{lo}));
            }
        }

        int64_t start_kafka_offset = _kafka_offsets[start_log_offset];
        for (int64_t ko = start_kafka_offset; ko < _log_offsets.size(); ++ko) {
            BOOST_TEST_CONTEXT("With kafka offset: " << ko) {
                BOOST_REQUIRE_EQUAL(
                  _log_offsets[ko],
                  _tr->state()->to_log_offset(model::offset{ko}));
            }
        }
    }

    static const std::vector<model::record_batch_type> all_batch_types;

    std::function<raft::offset_translator()> _make_offset_translator;

    std::optional<raft::offset_translator> _tr;
    ss::gate _gate;

    storage::log _log;

    model::offset _snapshot_offset;
    int64_t _snapshot_delta = 0;

    // kafka offsets for each log offset from 0 to tip of the log.
    std::vector<model::offset> _kafka_offsets;
    // log_offsets for each kafka offset from 0 to last kafka offset.
    std::vector<model::offset> _log_offsets;
};

const std::vector<model::record_batch_type> fuzz_checker::all_batch_types{
  model::record_batch_type::raft_data,
  model::record_batch_type::raft_configuration,
  model::record_batch_type::checkpoint,
};

FIXTURE_TEST(fuzz_operations_test, base_fixture) {
    constexpr int number_of_ops = 100;
    constexpr int number_of_runs = 10;

    enum class op_type {
        append,
        truncate,
        prefix_truncate,
        stop,
        restart,
        max, // should always be last
    };

    for (size_t i_run = 0; i_run < number_of_runs; ++i_run) {
        auto ntp = test_ntp;
        ntp.tp.topic = model::topic(fmt::format("{}_{}", ntp.tp.topic, i_run));
        auto log = _api.local()
                     .log_mgr()
                     .manage(storage::ntp_config(ntp, _test_dir))
                     .get();
        fuzz_checker checker(log, [this] { return make_offset_translator(); });
        checker.start().get();

        for (size_t i_op = 0; i_op < number_of_ops; ++i_op) {
            op_type op = static_cast<op_type>(random_generators::get_int(
              static_cast<size_t>(op_type::max) - 1));

            switch (op) {
            case op_type::append:
                checker.append().get();
                break;
            case op_type::truncate:
                checker.truncate().get();
                break;
            case op_type::prefix_truncate:
                checker.prefix_truncate().get();
                break;
            case op_type::stop:
                checker.stop().get();
                break;
            case op_type::restart:
                checker.restart().get();
                break;
            default:
                throw std::runtime_error{
                  fmt::format("unknown op type: {}", static_cast<size_t>(op))};
            }

            checker.validate();
        }

        checker.restart().get();
        checker.validate();
        checker.stop().get();
    }
}

FIXTURE_TEST(test_moving_persistent_state, base_fixture) {
    std::set<model::offset> batch_offsets{
      // data batch @ 0 -> kafka 0
      // data batch @ 1 -> kafka 1
      model::offset(2),
      model::offset(3),
      model::offset(4),
      model::offset(5),
      model::offset(6),
      // data batch @ 7 -> kafka 2
      model::offset(8),
      // data batch @ 9 -> kafka 3
    };
    auto local_ot = make_offset_translator();
    local_ot
      .start(
        raft::offset_translator::must_reset::yes,
        raft::offset_translator::bootstrap_state{})
      .get();
    for (auto o : batch_offsets) {
        local_ot.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    // process some fake large batches to force offset translator checkpoint
    for (auto i = 0; i < 10; ++i) {
        auto large_batch = create_batch(
          model::record_batch_type::raft_data, model::offset(9 + i));
        large_batch.header().size_bytes = 8_MiB;
        local_ot.process(large_batch);
    }

    local_ot.maybe_checkpoint().get();

    validate_translation(local_ot, model::offset(0), model::offset(0));
    validate_translation(local_ot, model::offset(1), model::offset(1));
    validate_translation(local_ot, model::offset(7), model::offset(2));
    validate_translation(local_ot, model::offset(9), model::offset(3));
    validate_translation(local_ot, model::offset(10), model::offset(4));
    validate_translation(local_ot, model::offset(11), model::offset(5));

    // use last available shard
    auto target_shard = ss::smp::count - 1;
    // move state to target shard
    raft::offset_translator::move_persistent_state(
      raft::group_id(0), ss::this_shard_id(), target_shard, _api)
      .get();

    // validate translation on target shard
    ss::smp::submit_to(
      target_shard,
      [&api = _api, ntp = test_ntp]() -> ss::future<> {
          auto remote_ot = raft::offset_translator{
            {model::record_batch_type::raft_configuration,
             model::record_batch_type::checkpoint},
            raft::group_id(0),
            ntp,
            api.local()};
          return ss::do_with(std::move(remote_ot), [](auto& remote_ot) {
              return remote_ot
                .start(
                  raft::offset_translator::must_reset::no,
                  raft::offset_translator::bootstrap_state{})
                .then([&remote_ot] {
                    validate_translation(
                      remote_ot, model::offset(0), model::offset(0));
                    validate_translation(
                      remote_ot, model::offset(1), model::offset(1));
                    validate_translation(
                      remote_ot, model::offset(7), model::offset(2));
                    validate_translation(
                      remote_ot, model::offset(9), model::offset(3));
                    validate_translation(
                      remote_ot, model::offset(10), model::offset(4));
                    validate_translation(
                      remote_ot, model::offset(11), model::offset(5));
                });
          });
      })
      .get();

    // check if keys were deleted
    auto map = _api.local().kvs().get(
      storage::kvstore::key_space::offset_translator,
      local_ot.offsets_map_key());
    auto highest_known_offset = _api.local().kvs().get(
      storage::kvstore::key_space::offset_translator,
      local_ot.highest_known_offset_key());

    BOOST_REQUIRE_EQUAL(map.has_value(), false);
    BOOST_REQUIRE_EQUAL(highest_known_offset.has_value(), false);
}
