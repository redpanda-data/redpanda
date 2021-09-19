// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/offset_translator.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/fixture.h"

using namespace kafka;                // NOLINT
using namespace std::chrono_literals; // NOLINT

struct offset_translator_fixture {
    offset_translator_fixture()
      : _test_dir(
        fmt::format("test_{}", random_generators::gen_alphanum_string(6)))
      , _api(make_kv_cfg(), make_log_cfg())
      , _log(raft::group_id(0), test_ntp)
      , config_manager(
          raft::group_configuration({}, model::revision_id(0)),
          raft::group_id(0),
          _api,
          _log)
      , tr(config_manager) {
        _api.start().get();
    }

    storage::kvstore_config make_kv_cfg() const {
        return storage::kvstore_config(
          1_MiB, 10ms, _test_dir, storage::debug_sanitize_files::yes);
    }

    storage::log_config make_log_cfg() const {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          _test_dir,
          100_MiB,
          storage::debug_sanitize_files::yes);
    }
    model::ntp test_ntp = model::ntp(
      model::ns("test"), model::topic("tp"), model::partition_id(0));
    ss::sstring _test_dir;
    raft::ctx_log _log;
    storage::api _api;
    raft::configuration_manager config_manager;
    offset_translator tr;

    ~offset_translator_fixture() { _api.stop().get(); }

    void validate_offset_translation(
      model::offset log_offset, model::offset kafka_offset) {
        BOOST_REQUIRE_EQUAL(tr.to_kafka_offset(log_offset), kafka_offset);
        BOOST_REQUIRE_EQUAL(tr.from_kafka_offset(kafka_offset), log_offset);
    }
};

FIXTURE_TEST(test_translating_to_kafka_offsets, offset_translator_fixture) {
    std::vector<raft::offset_configuration> cfgs;
    std::set<model::offset> config_offsets{
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
    cfgs.reserve(config_offsets.size());

    for (auto& o : config_offsets) {
        cfgs.emplace_back(
          o, raft::group_configuration({}, model::revision_id(0)));
    }

    config_manager.add(std::move(cfgs)).get();
    validate_offset_translation(model::offset(2), model::offset(0));
    validate_offset_translation(model::offset(6), model::offset(1));
    validate_offset_translation(model::offset(9), model::offset(2));
    validate_offset_translation(model::offset(11), model::offset(3));
    validate_offset_translation(model::offset(12), model::offset(4));
}

FIXTURE_TEST(
  test_translating_to_kafka_offsets_first, offset_translator_fixture) {
    std::vector<raft::offset_configuration> cfgs;
    std::set<model::offset> config_offsets{// data batch @ 0 -> kafka 0
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
    cfgs.reserve(config_offsets.size());

    for (auto& o : config_offsets) {
        cfgs.emplace_back(
          o, raft::group_configuration({}, model::revision_id(0)));
    }

    config_manager.add(std::move(cfgs)).get();
    validate_offset_translation(model::offset(0), model::offset(0));
    validate_offset_translation(model::offset(1), model::offset(1));
    validate_offset_translation(model::offset(7), model::offset(2));
    validate_offset_translation(model::offset(9), model::offset(3));
    validate_offset_translation(model::offset(11), model::offset(4));
}

FIXTURE_TEST(random_translation_test, offset_translator_fixture) {
    auto cfgs_count = 1000;
    std::vector<raft::offset_configuration> cfgs;
    std::set<model::offset> config_offsets;
    cfgs.reserve(config_offsets.size());
    /**
     * control batches in random places
     */
    for (auto i = 0; i < cfgs_count; ++i) {
        config_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto& o : config_offsets) {
        cfgs.emplace_back(
          o, raft::group_configuration({}, model::revision_id(0)));
    }

    config_manager.add(std::move(cfgs)).get();
    // go over whole offset space
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (config_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.to_kafka_offset(log_offset);
        auto reverse_log_offset = tr.from_kafka_offset(kafka_offset);
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(random_translation_test_with_hint, offset_translator_fixture) {
    auto cfgs_count = 1000;
    std::vector<raft::offset_configuration> cfgs;
    std::set<model::offset> config_offsets;
    cfgs.reserve(config_offsets.size());
    /**
     * control batches in random places
     */
    for (auto i = 0; i < cfgs_count; ++i) {
        config_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto& o : config_offsets) {
        cfgs.emplace_back(
          o, raft::group_configuration({}, model::revision_id(0)));
    }

    config_manager.add(std::move(cfgs)).get();
    // go over whole offset space
    model::offset prev_log_offset;
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (config_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.to_kafka_offset(log_offset);
        auto reverse_log_offset = tr.from_kafka_offset(
          kafka_offset, prev_log_offset);
        prev_log_offset = reverse_log_offset;
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(immutabitlity_test, offset_translator_fixture) {
    auto cfgs_count = 100;
    std::vector<raft::offset_configuration> cfgs;
    std::set<model::offset> config_offsets;
    cfgs.reserve(config_offsets.size());
    /**
     * control batches in random places
     */
    for (auto i = 0; i < cfgs_count; ++i) {
        config_offsets.emplace(random_generators::get_int(1000));
    }

    for (auto& o : config_offsets) {
        cfgs.emplace_back(
          o, raft::group_configuration({}, model::revision_id(0)));
    }

    config_manager.add(std::move(cfgs)).get();
    // internal offset -> kafka offset
    std::unordered_map<model::offset, model::offset> offsets_mapping;

    // go over whole offset space
    for (auto o : boost::irange(0, 1100)) {
        model::offset log_offset(o);

        if (config_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.to_kafka_offset(log_offset);
        auto reverse_log_offset = tr.from_kafka_offset(kafka_offset);

        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
        offsets_mapping.emplace(log_offset, kafka_offset);
    }

    auto validate_offsets_immutable = [&](int64_t start) {
        for (auto o : boost::irange<int64_t>(start, 1100)) {
            model::offset log_offset(o);
            if (config_offsets.contains(log_offset)) {
                continue;
            }
            model::offset k_offset = tr.to_kafka_offset(log_offset);
            // validate that offset havent changed
            BOOST_REQUIRE_EQUAL(offsets_mapping[log_offset], k_offset);
        }
    };

    // prefix truncate
    auto truncate_at = random_generators::get_int(1100 - 1);
    config_manager.prefix_truncate(model::offset(truncate_at)).get();

    validate_offsets_immutable(truncate_at + 1);

    // add new config at the end
    config_manager
      .add(
        model::offset(1101),
        raft::group_configuration({}, model::revision_id(0)))
      .get();

    validate_offsets_immutable(truncate_at + 1);
}
